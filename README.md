# Procesamiento de Datos de E-commerce con PySpark

![PySpark](https://img.shields.io/badge/PySpark-3.3.1-red)
![Python](https://img.shields.io/badge/Python-3.8+-blue)
![Big Data](https://img.shields.io/badge/Big%20Data-Processing-yellowgreen)

Proyecto de limpieza y preparación de datos de eventos de e-commerce utilizando PySpark para análisis posterior.

## 📋 Descripción

Este proyecto procesa datos de eventos de usuarios en un sitio de e-commerce, realizando limpieza, tratamiento de valores nulos, detección de outliers y normalización de datos para prepararlos para análisis.

## 🛠️ Tecnologías

- PySpark 3.3.1
- Python 3.8+
- Spark SQL
- Procesamiento distribuido

# Estructura del Proyecto

| 📁 **Carpeta/Archivo**       | 📄 **Contenido**                                                                 | 🏷️ **Tipo**         |
|-----------------------------|---------------------------------------------------------------------------------|--------------------|
| `archive/`                 | Archivos CSV originales descargados (datos brutos)                              | 🗃️ Datos de entrada |
| `clean_data/`              | Datos procesados y limpios (generados automáticamente)                          | 🧹 Datos de salida  |
| `data_cleaning.ipynb`                | Código de preparación de datos para el análisis.                                    | 🐍 Código fuente    |
| `analysis.ipynb`                | Código de con análisis de co-ocurrencia de productos.                                 | 🐍 Código fuente    |
| `README.md`                | Documentación del proyecto (este archivo)                                       | 📖 Documentación    |




## 🔍 Procesamiento de Datos

### 1️⃣ Tratamiento de Valores Nulos
- **Columnas afectadas**: `category_code`, `brand`, `user_session`
- **Acciones**:
  - Imputación con "unknown" para `category_code` y `brand`
  - Eliminación de registros con `user_session` nulo

### 2️⃣ Manejo de Outliers
- **Precios**:
  - Eliminación de precios negativos
  - Filtrado de precios fuera del rango (0.01 - 10,000)
  - Análisis de percentiles (IQR y P99)
  
- **Actividad de usuarios**:
  - Eliminación de usuarios con >150 eventos/día
  - Justificación: Eliminación de posible actividad bot

- **Duración de sesiones**:
  - Eliminación de sesiones con duración >15 días
  - Justificación: Sesiones prolongadas probablemente son errores de tracking

### 3️⃣ Normalización
- **Marcas (`brand`)**:
  - Conversión a minúsculas
  - Eliminación de espacios adicionales
  - Conservación de caracteres especiales (cuando son legítimos)

- **Fechas (`event_time`)**:
  - Extracción de componentes temporales (año, mes, día, hora)
  - Verificación de formato UTC



