#!/usr/bin/env python3
"""
Cliente WebSocket independiente para etl_analysis.

Este cliente se conecta al servidor de monitoreo y ejecuta etl_analysis
automáticamente cuando se detectan cambios en archivos .xlsx.
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import aiohttp
except ImportError:
    try:
        import urllib.request
        import urllib.parse
        aiohttp = None
    except ImportError:
        aiohttp = None

try:
    import websockets
except ImportError:
    print("Error: websockets no está instalado. Instala con: pip install websockets --user")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════

# URL del servidor WebSocket
WS_SERVER_URL = os.getenv("WS_SERVER_URL", "ws://10.4.0.2:8765")

# Directorio del proyecto etl_analysis
ETL_ANALYSIS_DIR = os.getenv("ETL_ANALYSIS_DIR", "/home/fits/codigo/Desktop/etl_analysis")

# URL del servidor HTTP de scan (en lugar de ejecutar comando directamente)
ETL_ANALYSIS_SCAN_URL = os.getenv("ETL_ANALYSIS_SCAN_URL", "http://127.0.0.1:3002/scan")

# Intervalos de tiempo
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "5"))  # Segundos antes de reintentar
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "30"))  # Segundos entre pings

# Agrupación: esperar N segundos acumulando cambios antes de ejecutar
BATCH_DELAY = int(os.getenv("BATCH_DELAY", "5"))  # Segundos de agrupación

# Extensiones de archivos a procesar para creación/modificación (solo .xlsx para analysis)
# Para eliminaciones, también procesamos .knfo y .meta
PROCESS_EXTENSIONS = ['.xlsx']
PROCESS_DELETE_EXTENSIONS = ['.xlsx', '.knfo', '.meta']  # Extensiones a procesar cuando se eliminan

# Configurar logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
log_file = Path(ETL_ANALYSIS_DIR) / 'websocket_analysis_client.log'
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# CLASE PRINCIPAL
# ═══════════════════════════════════════════════════════════════

class AnalysisWebSocketClient:
    """Cliente WebSocket que ejecuta etl_analysis cuando se detectan cambios en .xlsx."""

    def __init__(self, server_url: str, etl_analysis_dir: str, etl_analysis_cmd: str = None):
        self.server_url = server_url
        self.etl_analysis_dir = Path(etl_analysis_dir)
        self.running = False
        self.websocket: Optional[websockets.WebSocketServerProtocol] = None
        
        # Agrupación: acumular cambios durante BATCH_DELAY segundos
        self.last_change_time = None
        self.batch_task: Optional[asyncio.Task] = None
        self.pending_changes = set()  # Set de archivos que han cambiado
        self.batch_timer_running = False
        
        # Estadísticas
        self.stats = {
            'events_received': 0,
            'analysis_executed': 0,
            'errors': 0,
            'last_execution': None
        }

    async def batched_execute(self):
        """Ejecuta etl_analysis después de acumular cambios durante BATCH_DELAY segundos."""
        # Esperar el tiempo de agrupación
        await asyncio.sleep(BATCH_DELAY)
        
        # Verificar si hay cambios pendientes
        if self.pending_changes:
            files_to_process = self.pending_changes.copy()
            count = len(files_to_process)
            
            # Analizar tipos de archivos para el mensaje de log
            extensions = {}
            for file_path in files_to_process:
                ext = Path(file_path).suffix.lower()
                extensions[ext] = extensions.get(ext, 0) + 1
            
            ext_summary = ', '.join([f"{count_ext} {ext}" for ext, count_ext in sorted(extensions.items())])
            self.pending_changes.clear()
            self.batch_timer_running = False
            
            logger.info(f"📦 Ejecutando etl_analysis agrupado para {count} archivo(s) cambiado(s): {ext_summary}")
            await self.execute_analysis(files_to_process)
        else:
            self.batch_timer_running = False

    async def schedule_analysis(self, file_path: str):
        """
        Agrupa el archivo para análisis. Si no hay timer corriendo, inicia uno.
        Todos los cambios durante BATCH_DELAY segundos se ejecutarán juntos.
        
        Args:
            file_path: Ruta del archivo que cambió
        """
        self.pending_changes.add(file_path)
        self.last_change_time = datetime.now()
        
        # Si ya hay un timer corriendo, solo agregamos el archivo a la lista
        if not self.batch_timer_running:
            self.batch_timer_running = True
            logger.debug(f"📦 Iniciando agrupación de cambios (esperando {BATCH_DELAY}s para más cambios)")
            
            # Crear nueva tarea que esperará BATCH_DELAY segundos
            self.batch_task = asyncio.create_task(self.batched_execute())
        else:
            # Timer ya está corriendo, solo agregamos este archivo
            logger.debug(f"📦 Archivo agregado al lote actual ({len(self.pending_changes)} archivo(s) en espera)")

    async def execute_analysis(self, changed_files: set):
        """
        Envía señal HTTP para activar scan en etl_analysis.
        
        Args:
            changed_files: Set de rutas de archivos que han cambiado
        """
        try:
            # Analizar tipos de archivos para el mensaje de log
            extensions = {}
            for file_path in changed_files:
                ext = Path(file_path).suffix.lower()
                extensions[ext] = extensions.get(ext, 0) + 1
            
            ext_summary = ', '.join([f"{count_ext} {ext}" for ext, count_ext in sorted(extensions.items())])
            logger.info(f"🔬 Enviando señal de scan a etl_analysis para {len(changed_files)} archivo(s): {ext_summary}")
            
            # Enviar petición HTTP POST al servidor de scan
            scan_url = os.getenv("ETL_ANALYSIS_SCAN_URL", ETL_ANALYSIS_SCAN_URL)
            
            if aiohttp:
                async with aiohttp.ClientSession() as session:
                    async with session.post(scan_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            result = await response.json()
                            logger.info(f"✅ Señal de scan enviada exitosamente: {result.get('message', 'OK')}")
                            self.stats['analysis_executed'] += 1
                        else:
                            error_text = await response.text()
                            logger.error(f"❌ Error enviando señal de scan (status {response.status}): {error_text[:500]}")
                            self.stats['errors'] += 1
            else:
                # Fallback con urllib
                import urllib.request
                import urllib.parse
                req = urllib.request.Request(scan_url, method='POST')
                try:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        if response.status == 200:
                            logger.info(f"✅ Señal de scan enviada exitosamente")
                            self.stats['analysis_executed'] += 1
                        else:
                            logger.error(f"❌ Error enviando señal de scan (status {response.status})")
                            self.stats['errors'] += 1
                except Exception as e:
                    logger.error(f"❌ Error enviando señal de scan: {e}")
                self.stats['errors'] += 1
            
            self.stats['last_execution'] = datetime.now().isoformat()
            
        except Exception as e:
            logger.error(f"❌ Error enviando señal de scan: {e}", exc_info=True)
            self.stats['errors'] += 1

    async def process_file_event(self, event_data: dict):
        """
        Procesa un evento de cambio de archivo.
        
        Args:
            event_data: Diccionario con información del evento
        """
        event_type = event_data.get("event")
        
        self.stats['events_received'] += 1
        
        # Manejar evento agrupado files_changed
        if event_type == "files_changed":
            files = event_data.get("files", {})
            summary = event_data.get("summary", {})
            
            logger.info(f"📦 Evento files_changed recibido: {summary.get('total_files', 0)} archivo(s) - "
                       f"Creados: {summary.get('created', 0)}, "
                       f"Modificados: {summary.get('modified', 0)}, "
                       f"Eliminados: {summary.get('deleted', 0)}")
            
            # Procesar archivos creados (solo .xlsx, NO .knfo/.meta - esos los maneja upsert)
            for file_info in files.get("created", []):
                file_path = file_info.get("file_path", "")
                extension = file_info.get("extension", "")
                if extension == '.xlsx':
                    logger.info(f"📄 Archivo .xlsx nuevo detectado: {file_path}")
                    await self.schedule_analysis(file_path)
                elif extension in ['.knfo', '.meta']:
                    logger.debug(f"📄 Archivo {extension} nuevo detectado (ignorado por analysis): {file_path}")
                    # No ejecutar analysis cuando se crean .knfo/.meta - esos los maneja upsert
            
            # Procesar archivos modificados (solo .xlsx, NO .knfo/.meta - esos los maneja upsert)
            for file_info in files.get("modified", []):
                file_path = file_info.get("file_path", "")
                extension = file_info.get("extension", "")
                if extension == '.xlsx':
                    logger.info(f"✏️ Archivo .xlsx modificado detectado: {file_path}")
                    await self.schedule_analysis(file_path)
                elif extension in ['.knfo', '.meta']:
                    logger.debug(f"✏️ Archivo {extension} modificado detectado (ignorado por analysis): {file_path}")
                    # No ejecutar analysis cuando se modifican .knfo/.meta - esos los maneja upsert
            
            # Procesar archivos eliminados (cualquier archivo relacionado: .xlsx, .knfo, .meta)
            for file_info in files.get("deleted", []):
                file_path = file_info.get("file_path", "")
                extension = file_info.get("extension", "")
                if extension in ['.xlsx', '.knfo', '.meta']:
                    logger.info(f"🗑️ Archivo {extension} eliminado detectado: {file_path}")
                    await self.schedule_analysis(file_path)
            
            return
        
        # Manejar eventos individuales (formato antiguo)
        file_path = event_data.get("file_path", "")
        extension = event_data.get("extension", "")
        
        # Procesar según el tipo de evento
        if event_type == "file_created":
            if extension == '.xlsx':
                logger.info(f"📄 Archivo .xlsx nuevo detectado: {file_path}")
                await self.schedule_analysis(file_path)
            elif extension in ['.knfo', '.meta']:
                logger.debug(f"📄 Archivo {extension} nuevo detectado (ignorado por analysis): {file_path}")
                # No ejecutar analysis cuando se crean .knfo/.meta - esos los maneja upsert
            else:
                logger.debug(f"Extensión {extension} ignorada (solo procesamos .xlsx en creación)")
            
        elif event_type == "file_modified":
            if extension == '.xlsx':
                logger.info(f"✏️ Archivo .xlsx modificado detectado: {file_path}")
                await self.schedule_analysis(file_path)
            elif extension in ['.knfo', '.meta']:
                logger.debug(f"✏️ Archivo {extension} modificado detectado (ignorado por analysis): {file_path}")
                # No ejecutar analysis cuando se modifican .knfo/.meta - esos los maneja upsert
            else:
                logger.debug(f"Extensión {extension} ignorada (solo procesamos .xlsx en modificación)")
            
        elif event_type == "file_deleted":
            # Ejecutar análisis cuando se elimina cualquier archivo relacionado (.xlsx, .knfo, .meta)
            if extension in ['.xlsx', '.knfo', '.meta']:
                logger.info(f"🗑️ Archivo {extension} eliminado detectado: {file_path}")
                await self.schedule_analysis(file_path)
            else:
                logger.debug(f"Extensión {extension} ignorada en eliminación")
            
        elif event_type == "connected":
            logger.info("✅ Conectado al servidor de monitoreo")
            logger.info(f"   Esperando cambios en archivos: .xlsx")
            
        elif event_type == "pong":
            logger.debug("Pong recibido del servidor")
        else:
            logger.debug(f"Evento desconocido: {event_type}")

    async def connect(self):
        """Conecta al servidor WebSocket."""
        while self.running:
            try:
                logger.info(f"🔌 Conectando a {self.server_url}...")
                async with websockets.connect(
                    self.server_url,
                    ping_interval=PING_INTERVAL,
                    ping_timeout=10
                ) as websocket:
                    self.websocket = websocket
                    logger.info("✅ Conectado exitosamente")
                    
                    # Enviar ping periódico
                    ping_task = asyncio.create_task(self.send_ping_loop())
                    
                    # Escuchar mensajes
                    async for message in websocket:
                        try:
                            event_data = json.loads(message)
                            await self.process_file_event(event_data)
                        except json.JSONDecodeError as e:
                            logger.error(f"Error decodificando mensaje: {e}")
                        except Exception as e:
                            logger.error(f"Error procesando evento: {e}", exc_info=True)
                    
                    ping_task.cancel()
                    
            except websockets.exceptions.ConnectionClosed:
                logger.warning("⚠️ Conexión cerrada por el servidor")
                if self.running:
                    logger.info(f"🔄 Reintentando en {RECONNECT_DELAY} segundos...")
                    await asyncio.sleep(RECONNECT_DELAY)
            
            except Exception as e:
                logger.error(f"❌ Error de conexión: {e}")
                if self.running:
                    logger.info(f"🔄 Reintentando en {RECONNECT_DELAY} segundos...")
                    await asyncio.sleep(RECONNECT_DELAY)

    async def send_ping_loop(self):
        """Envía pings periódicos al servidor."""
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                if self.websocket:
                    ping_message = {
                        "action": "ping",
                        "timestamp": datetime.now().isoformat()
                    }
                    await self.websocket.send(json.dumps(ping_message))
                    logger.debug("Ping enviado al servidor")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error en ping loop: {e}")

    def print_stats(self):
        """Imprime estadísticas del cliente."""
        logger.info("=" * 60)
        logger.info("ESTADÍSTICAS DEL CLIENTE ETL_ANALYSIS")
        logger.info("=" * 60)
        logger.info(f"Eventos recibidos: {self.stats['events_received']}")
        logger.info(f"etl_analysis ejecutados: {self.stats['analysis_executed']}")
        logger.info(f"Errores: {self.stats['errors']}")
        logger.info(f"Última ejecución: {self.stats['last_execution'] or 'Nunca'}")
        logger.info("=" * 60)

    def start(self):
        """Inicia el cliente."""
        self.running = True
        logger.info("=" * 60)
        logger.info("INICIANDO CLIENTE WEBSOCKET ETL_ANALYSIS")
        logger.info("=" * 60)
        logger.info(f"Servidor: {self.server_url}")
        logger.info(f"Directorio: {self.etl_analysis_dir}")
        logger.info(f"URL de scan: {ETL_ANALYSIS_SCAN_URL}")
        logger.info(f"Agrupación: {BATCH_DELAY} segundos")
        logger.info(f"Extensiones a procesar: .xlsx (creación/modificación), .xlsx/.knfo/.meta (eliminación)")
        logger.info(f"Extensiones ignoradas: .knfo/.meta (creación/modificación) - manejadas por upsert")
        logger.info("=" * 60)
        
        # Verificar que el directorio existe
        if not self.etl_analysis_dir.exists():
            logger.error(f"❌ El directorio etl_analysis no existe: {self.etl_analysis_dir}")
            sys.exit(1)
        
        asyncio.run(self.connect())

    def stop(self):
        """Detiene el cliente."""
        self.running = False
        logger.info("🛑 Deteniendo cliente WebSocket...")
        
        # Si hay cambios pendientes, ejecutarlos antes de detener
        if self.pending_changes and self.batch_timer_running:
            logger.info(f"Ejecutando etl_analysis final para {len(self.pending_changes)} archivo(s) pendiente(s)...")
            asyncio.create_task(self.execute_analysis(self.pending_changes.copy()))
            self.pending_changes.clear()
        
        if self.batch_task and not self.batch_task.done():
            self.batch_task.cancel()
        self.print_stats()

# ═══════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def main():
    """Función principal."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Cliente WebSocket para ejecutar etl_analysis cuando se detectan cambios en .xlsx',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--url',
        default=WS_SERVER_URL,
        help=f'URL del servidor WebSocket (default: {WS_SERVER_URL})'
    )
    
    parser.add_argument(
        '--etl-analysis-dir',
        default=ETL_ANALYSIS_DIR,
        help=f'Directorio del proyecto etl_analysis (default: {ETL_ANALYSIS_DIR})'
    )
    
    parser.add_argument(
        '--scan-url',
        default=ETL_ANALYSIS_SCAN_URL,
        help=f'URL del servidor HTTP de scan (default: {ETL_ANALYSIS_SCAN_URL})'
    )
    
    parser.add_argument(
        '--batch-delay',
        type=int,
        default=BATCH_DELAY,
        help=f'Tiempo de agrupación en segundos (default: {BATCH_DELAY})'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=LOG_LEVEL,
        help=f'Nivel de logging (default: {LOG_LEVEL})'
    )
    
    args = parser.parse_args()
    
    # Actualizar variable de entorno si se especificó
    if args.batch_delay != BATCH_DELAY:
        os.environ["BATCH_DELAY"] = str(args.batch_delay)
    
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    client = AnalysisWebSocketClient(
        args.url,
        args.etl_analysis_dir
    )
    
    try:
        client.start()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupción recibida, deteniendo...")
        client.stop()
    except Exception as e:
        logger.error(f"❌ Error fatal: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

