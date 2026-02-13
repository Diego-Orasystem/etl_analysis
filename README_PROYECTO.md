# 📊 Proyecto ETL Analysis

## 🎯 Propósito

El proyecto **`etl_analysis`** es un sistema ETL (Extract, Transform, Load) especializado en **analizar archivos Excel** y generar archivos de metadatos (`.knfo` y `.meta`) que describen la estructura y contenido de los archivos para su posterior procesamiento.

---

## 🏗️ Arquitectura

### Estructura del Proyecto

```
etl_analysis/
├── excelAutoKNFO.js          # ⭐ Archivo principal - Orquestador
├── sftpPool.js                # Pool de conexiones SFTP (thread-safe)
├── analysisHelpers.js         # Funciones auxiliares
├── package.json               # Dependencias y scripts
└── analysisProcess/           # Módulos de procesamiento
    ├── analyzeExcelFile.js    # Análisis profundo de archivos Excel
    ├── buildKnfo.js           # Generación de archivos .knfo
    ├── buildMeta.js           # Generación de archivos .meta
    ├── detectKeyColumns.js    # Detección de columnas clave
    ├── determinePossibleUses.js # Determinación de tipos de datos
    ├── parallelXlsxReader.js   # Lectura paralela de Excel
    ├── datetimeUtil.js        # Utilidades de fecha/hora
    ├── numberUtil.js          # Utilidades numéricas
    └── ...
```

---

## 🔄 Flujo de Procesamiento

### 1. **Descubrimiento (Discovery)**
```
scanCtx(ctx) → Escanea directorios SFTP
  ├─ Lista archivos .xlsx en excelDir
  ├─ Lista archivos .knfo existentes en knfoDir
  └─ Lista archivos .meta existentes en metaRoot
```

### 2. **Filtrado**
- Solo procesa archivos que coinciden con `PATTERNS` (MB51, ME5A, LEK2DAT_FORECAST, etc.)
- Omite archivos que ya tienen `.knfo` y `.meta` generados
- Prioriza archivos grandes (>= 20MB) para procesamiento exclusivo

### 3. **Procesamiento Paralelo**
```
Worker Thread por archivo:
  ├─ analyzeExcelFile() → Análisis profundo del Excel
  ├─ buildKnfo() → Genera archivo .knfo (estructura)
  └─ buildMeta() → Genera archivo .meta (metadatos)
```

### 4. **Subida a SFTP**
- Sube los archivos `.knfo` y `.meta` generados al servidor SFTP
- Los archivos quedan listos para el siguiente paso del pipeline ETL

---

## ⚙️ Componentes Principales

### 📄 `excelAutoKNFO.js` (Orquestador Principal)

**Responsabilidades:**
- ✅ Escanea directorios SFTP periódicamente (cada 5 segundos por defecto)
- ✅ Gestiona colas de trabajo por contexto (CAN/CAS)
- ✅ Procesa archivos en paralelo usando Worker Threads
- ✅ Prioriza archivos grandes para procesamiento exclusivo
- ✅ Maneja errores y reintentos

**Características:**
- **Worker Threads**: Cada archivo se procesa en un thread separado
- **Pool SFTP**: Usa `sftpPool.js` para conexiones thread-safe
- **Priorización**: Archivos >= 20MB se procesan exclusivamente
- **Paralelismo**: Hasta 5 archivos pequeños simultáneos (configurable)

### 🔌 `sftpPool.js` (Pool de Conexiones SFTP)

**Características:**
- ✅ **Thread-safe**: Serializa acceso a conexiones SFTP
- ✅ **Pool de conexiones**: Máximo 10 conexiones reutilizables
- ✅ **Circuit Breaker**: Evita conexiones cuando el servidor está caído
- ✅ **Auto-reconexión**: Reconstruye conexiones muertas automáticamente
- ✅ **Timeouts**: Desconecta conexiones inactivas después de 90 segundos
- ✅ **Retry logic**: Reintentos automáticos con backoff exponencial

### 🔬 `analysisProcess/analyzeExcelFile.js`

**Funcionalidad:**
- Analiza la estructura completa del archivo Excel
- Detecta tipos de datos por columna
- Identifica patrones y correlaciones
- Genera estadísticas detalladas
- Detecta columnas clave (key columns)
- Analiza frecuencias y valores únicos

### 📋 `analysisProcess/buildKnfo.js`

**Genera archivos `.knfo`:**
- Estructura de columnas normalizadas
- Tipos de datos inferidos
- Relaciones entre columnas
- Metadatos de estructura

### 📊 `analysisProcess/buildMeta.js`

**Genera archivos `.meta`:**
- Metadatos completos del análisis
- Configuración de normalización
- Mapeo de columnas
- Información para generación de tablas

---

## 🚀 Configuración

### Variables de Entorno

```bash
# Intervalos
INTERVAL_MS=5000              # Intervalo de escaneo (ms)
PARALLEL_LIMIT=5              # Archivos procesados en paralelo
SFTP_TIMEOUT_MS=30000         # Timeout para operaciones SFTP

# Umbrales
XLSX_STREAM_MIN_MB=20        # Archivos >= 20MB se procesan exclusivamente

# Contextos
CTX_LIST=CAN,CAS              # Contextos a procesar

# SFTP (en sftpPool.js)
SFTP_HOST=10.4.0.2
SFTP_PORT=22
SFTP_USER=fits
SFTP_PASS=fits.2024

# Worker Threads
SFTP_PER_WORKER=1             # Cada worker tiene su propia conexión SFTP
```

### Patrones de Archivos

El sistema solo procesa archivos que contienen estos patrones en el nombre:

```javascript
PATTERNS = [
  'MB51', 'MB5B', 'ME5A', 'ME2L',
  'ZMMR_SQVI_BUS_RAPIDA', 'MRO_IO', 'ZMMREPO', 'S_P99_41000062',
  'KOB1', 'CJI3', 'KSB1', 'ZFIR_STATSLOAD', 'CN41N', 
  'ZRPT_PS_PROJECT', 'IW49N',
  'LEK2DAT_FORECAST', 'LEK2DAT_STRUCTURE_EE', 'LEK2DAT_STRUCTURE_CC',
  'LEK2DAT_STRUCTURE_CCEE', 'LEK2DAT_STRUCTURE_ACC',
  'PRESU', 'PATRI'
]
```

---

## 🔄 Ciclo de Vida

### Estados del Sistema

1. **BOOT**: Escaneo inicial al iniciar
2. **RESCAN**: Escaneos periódicos cada `INTERVAL_MS`
3. **IDLE**: Sin trabajo pendiente

### Flujo de Trabajo

```
1. Escaneo → Detecta archivos .xlsx sin .knfo/.meta
2. Encolado → Agrega a cola por contexto (CAN/CAS)
3. Selección → Pick batch según tamaño (LARGE exclusivo, pequeños en paralelo)
4. Procesamiento → Worker Thread por archivo
5. Generación → Crea .knfo y .meta
6. Subida → Sube archivos al SFTP
7. Repetición → Vuelve al paso 1
```

---

## 📊 Estadísticas y Monitoreo

El sistema registra:
- Archivos procesados por contexto
- Tiempo de procesamiento
- Errores y reintentos
- Archivos grandes vs pequeños
- Estado de las colas

---

## 🔗 Integración con Otros Proyectos

### Relación con `etl_upsert`

```
etl_analysis (este proyecto)
  ↓ Genera .knfo y .meta
  ↓
etl_upsert
  ↓ Lee .knfo y .meta
  ↓ Genera tablas en MariaDB
  ↓ Inserta datos
```

**Flujo completo:**
1. `etl_analysis` analiza Excel → genera `.knfo` y `.meta`
2. `etl_upsert` lee `.knfo` y `.meta` → crea tablas → inserta datos

---

## 🛠️ Tecnologías Utilizadas

- **Node.js** con Worker Threads para paralelismo
- **ExcelJS** y **XLSX** para lectura de Excel
- **ssh2-sftp-client** para acceso SFTP
- **Arquero** para análisis de datos
- **MySQL2** para base de datos (si aplica)

---

## 📈 Rendimiento

- **Paralelismo**: Hasta 5 archivos pequeños simultáneos
- **Priorización**: Archivos grandes se procesan exclusivamente
- **Pool SFTP**: Reutilización de conexiones (máx. 10)
- **Worker Threads**: Aislamiento de procesos por archivo

---

## 🔍 Diferencias con `etl_upsert`

| Característica | etl_analysis | etl_upsert |
|---------------|--------------|------------|
| **Propósito** | Analizar Excel y generar metadatos | Insertar datos en BD |
| **Entrada** | Archivos .xlsx | Archivos .xlsx + .knfo + .meta |
| **Salida** | Archivos .knfo y .meta | Tablas en MariaDB |
| **Fase** | Análisis y estructuración | Carga de datos |
| **Dependencias** | Requiere .xlsx | Requiere .xlsx, .knfo, .meta |

---

## ✅ Estado Actual

- ✅ Sistema funcional y en producción
- ✅ Procesamiento paralelo con Worker Threads
- ✅ Pool de conexiones SFTP thread-safe
- ✅ Manejo robusto de errores
- ✅ Priorización de archivos grandes
- ✅ Integración con pipeline ETL completo

---

## 📝 Notas Importantes

1. **Memoria**: El proceso usa hasta 32GB de memoria (`--max-old-space-size=32896`)
2. **SFTP**: Requiere acceso al servidor SFTP configurado
3. **Patrones**: Solo procesa archivos que coinciden con los patrones definidos
4. **Worker Threads**: Cada archivo se procesa en un thread aislado
5. **Pool SFTP**: Las conexiones se reutilizan para mejor rendimiento

---

## 🚀 Ejecución

```bash
# Desarrollo
npm start

# Debug (con inspector)
npm run debug

# Con variables de entorno
INTERVAL_MS=10000 PARALLEL_LIMIT=3 node excelAutoKNFO.js
```

---

Este proyecto es la **primera fase** del pipeline ETL completo, encargándose del análisis y estructuración de datos antes de su carga en la base de datos.


