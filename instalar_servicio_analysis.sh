#!/bin/bash
# Script para instalar el servicio WebSocket de etl_analysis

set -e

SERVICE_NAME="websocket-analysis-client"
SERVICE_FILE="/home/fits/codigo/Desktop/etl_analysis/websocket-analysis-client.service"
SYSTEMD_DIR="/etc/systemd/system"

echo "=========================================="
echo "Instalando servicio WebSocket ETL Analysis"
echo "=========================================="

# Verificar que el archivo de servicio existe
if [ ! -f "$SERVICE_FILE" ]; then
    echo "❌ Error: No se encuentra el archivo de servicio: $SERVICE_FILE"
    exit 1
fi

# Copiar el archivo de servicio
echo "📋 Copiando archivo de servicio..."
sudo cp "$SERVICE_FILE" "$SYSTEMD_DIR/$SERVICE_NAME.service"

# Recargar systemd
echo "🔄 Recargando systemd..."
sudo systemctl daemon-reload

# Habilitar el servicio
echo "✅ Habilitando servicio..."
sudo systemctl enable "$SERVICE_NAME.service"

# Iniciar el servicio
echo "🚀 Iniciando servicio..."
sudo systemctl start "$SERVICE_NAME.service"

# Verificar estado
echo ""
echo "📊 Estado del servicio:"
sudo systemctl status "$SERVICE_NAME.service" --no-pager -l

echo ""
echo "=========================================="
echo "✅ Instalación completada"
echo "=========================================="
echo ""
echo "Comandos útiles:"
echo "  Ver estado:    sudo systemctl status $SERVICE_NAME"
echo "  Ver logs:      sudo journalctl -u $SERVICE_NAME -f"
echo "  Reiniciar:     sudo systemctl restart $SERVICE_NAME"
echo "  Detener:       sudo systemctl stop $SERVICE_NAME"
echo ""


