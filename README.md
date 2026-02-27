# 📊 Auto Insight Dual Analysis Tool

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Polars](https://img.shields.io/badge/Polars-1.37%2B-orange?style=for-the-badge&logo=polars&logoColor=white)](https://pola.rs)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.53%2B-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Plotly](https://img.shields.io/badge/Plotly-6.5%2B-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)](https://plotly.com)
[![Pydantic](https://img.shields.io/badge/Pydantic-2.12%2B-E92063?style=for-the-badge&logo=pydantic&logoColor=white)](https://docs.pydantic.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](https://opensource.org/licenses/MIT)

## 🎯 ¿Qué problema resuelve?

¿Te ha pasado que...

- 📉 Tienes un CSV y no sabes por dónde empezar a analizarlo?
- 🔍 Quieres explorar datos rápido pero hacer filtros en pandas es tedioso?
- 📊 Necesitas insights automáticos sin escribir código cada vez?
- 🎨 Las visualizaciones estáticas no te dejan interactuar con los datos?

**Auto Insight Dual Analysis Tool** nace para resolver exactamente eso: **una herramienta dual** que combina lo mejor de dos mundos:

### 🔬 **Modo 1: Análisis Automatizado** -> Para cuando quieres respuestas rápidas

Carga tu CSV, configura qué quieres analizar (o déjalo en automático) y obtén:

- Reporte completo en terminal con insights clave
- Visualizaciones interactivas guardadas como HTML
- JSON con todos los hallazgos estructurados
- Detección inteligente de outliers, distribuciones y correlaciones

### 🎮 **Modo 2: Exploración Interactiva** -> Para cuando quieres jugar con los datos

Una app en Streamlit que te permite:

- Filtros dinámicos sin escribir código
- Operaciones al vuelo (suma, promedio, conteo...)
- Agrupaciones múltiples con un clic
- Visualizaciones que se actualizan en tiempo real
- Historial de filtros 

App Link: https://prism-77em.onrender.com

## 🚀 Ejemplo Rápido

### **Con 3 líneas en terminal:**

```bash
    # 1. Configuras (o usas el default)
    # 2. Ejecutas
    python main.py
    # 3. Obtienes (salida de ejemplo):
    === AUTOMATED INSIGHTS REPORT ===
    Dataset: vehicles_us.csv
    Columns: ['price', 'model_year', 'model', 'condition', 'cylinders', 'fuel', 'odometer', 'transmission', 'type', 'paint_color', 'is_4wd', 'date_posted', 'days_listed']
    
    1. 📈 DISTRIBUTION
    - price: media=12132.464919941776, median=9000.0, std=10040.803015443296, sesgo positive
    - Plot: /home/arbolitos7/Documents/TripleTen/Sprint7/Sprint7/analysis_report/analysis_2026-02-25/distribution/distribution_price.html
    - model_year: media=2009.75046966977, median=2011.0, std=6.2820647921742045, sesgo negative
    - Plot: /home/arbolitos7/Documents/TripleTen/Sprint7/Sprint7/analysis_report/analysis_2026-02-25/distribution/distribution_model_year.html
    - ...
    
    2. ⚠️ OUTLIERS
    - price: 1646 outliers -> (3.19%)
    - Sample: /home/arbolitos7/Documents/TripleTen/Sprint7/Sprint7/analysis_report/analysis_2026-02-25/outliers/outliers_price.html
    - model_year: 709 outliers -> (1.38%)
    - Sample: /home/arbolitos7/Documents/TripleTen/Sprint7/Sprint7/analysis_report/analysis_2026-02-25/outliers/outliers_model_year.html
    - ...

    3. 🔗 CORRELATION
    - The correlation value is invalid. Correlation detected: The columns ['price', 'model_year', 'cylinders', 'odometer', 'is_4wd', 'days_listed'] have zero standard deviation or is zero

    4. 🏷️ CATEGORIES
    - model: top lables= ['ford f-150', 'chevrolet silverado 1500', 'ram 1500', 'chevrolet silverado', 'jeep wrangler', 'ram 2500', 'toyota camry', 'honda accord', 'chevrolet silverado 2500hd', 'gmc sierra 1500']
    - model: 69 rare categories (<1.0%)
    - condition: top lables= ['excellent', 'good', 'like new', 'fair', 'new', 'salvage']
    - condition: 2 rare categories (<1.0%)
    - ...
```

### **Con 1 comando en tu terminal:**

```bash
    streamlit run app.py
```

## 🧠 ¿Cómo piensa DataAnalysis?

### **1. Inteligencia contextual**

No es un simple script: **entiende el tipo de dato** y actúa en consecuencia:

| Contexto               | Comportamiento                                                                          |
|------------------------|-----------------------------------------------------------------------------------------|
| **Columna numérica**   | Ofrece sum, avg, max, min, count + boxplot, histograma                                  |
| **Columna categórica** | Ofrece count, unique + histograma                                                       |
| **Heatmap**            | Si hay 1 columna, toma todas las numéricas automáticamente o puedes elegir las columnas |
| **Filtro numérico**    | Detecta rango y ofrece slider u operadores                                              |
| **Filtro categórico**  | Muestra valores únicos para selección o búsqueda                                        |

### **2. Auto-detección de insights**

En el modo automatizado, si no especificas columnas:

| Insight          | Si no hay columnas                        |
|------------------|-------------------------------------------|
| **Distribución** | Analiza TODAS las columnas                |
| **Outliers**     | Toma todas las NUMÉRICAS                  |
| **Correlación**  | Requiere ≥2 numéricas, si no -> desactiva |
| **Categorías**   | Toma todas las CATEGÓRICAS                |

## 🎨 Tour por la App Interactiva (ejemplo)

![Main Panel](assets/main-panel.png)

### 📍 **Sidebar: Tu centro de control**

| Sección              | ¿Qué hace?	                   | Ejemplo                        |
|----------------------|-------------------------------|--------------------------------|
| 📂 **Data Source**   | Muestra el archivo actual     | vehicles_us.csv                |
| 🧮 **Value Column**  | Columna a analizar (solo una) | price, model, condition        |
| 📊 **Operation**	   | Operación según tipo	       | Sum, Avg, Count, Unique        |
| 🗂️ **Group By**      | Agrupación múltiple	        | model_year + model             |
| 🎨 **Visualization** | Tipo de gráfico	           | histogram, boxplot, heatmap    |
| 🔍 **Filters**       | Filtros dinámicos	           | sliders, operadores, búsquedas |

### 🎯 **Panel Principal**

#### 1. Vista previa de datos

```python
    # Siempre ves las primeras 10 filas para contexto
    frame.head(10)
```

#### 2. Resultados en tiempo real

![Head DataFrame](assets/head-frame.png)

#### 3. Agrupaciones interactivas

Cuando agrupas, obtienes un DataFrame como este:

![Grouping DataFrame](assets/grouping-result.png)

Y un checkbox para **"Apply grouping frame"** que decide si el gráfico respeta la agrupación.

#### 4. Sistema de filtros profesional

**Filtros numéricos:**

- 🎚️ Slider: Rango continuo (min, max)
- ➕ Operadores: >, >=, <, <=, = con valor
- 🔍 Búsqueda: Escribe el valor exacto

![Numeric Filters](assets/numeric-filters.png)

**Filtros categóricos:**

- 🔍 Búsqueda: Escribe el valor exacto
- ✅ Selección múltiple: Escoge de valores existentes

![Categoric Filter](assets/categoric-filter.png)

**Historial con timestamp:**

![Historial Filter](assets/historial-filters.png)

## 📁 Output Organizado

Cada análisis genera su propia carpeta con fecha:

```text 
    analysis_report/
    ├── analysis_2026-02-11/
    │   ├── distribution/
    │   │   ├── distribution_price.html    
    │   │   ├── distribution_model.html     
    │   │   └── ...
    │   ├── outliers/
    │   │   ├── outliers_price.html         
    │   │   └── ...
    │   └── correlation/
    │       └── correlation_matrix.html     
    └── json_analysis/
        └── insights_2026-02-11.json        
```

## 🛠️ Stack Tecnológico

| Tecnología    | Por qué la elegimos                                      |
|---------------|----------------------------------------------------------|
| **Polars** 	| Procesamiento rápido en memoria, eager mode para control |
| **Pydantic** 	| Validación de configs con tipos y mensajes claros        |
| **Plotly**    | Gráficas interactivas que se guardan como HTML           |
| **Streamlit** | Prototipado rápido de dashboards profesionales           |
| **YAML/TOML** | Configuración humana vs configuración tipada             |
| **Enum** 	    | Estrategias extensibles, fácil añadir más                |

## 🚀 Instalación Rápida

```bash 
    # 1. Clona
    git clone https://github.com/sm7ss/auto-insight-dual-analysis-tool.git
    cd dataanalysis
    
    # 2. Instala (recomendado: entorno virtual)
    python -m venv venv
    source venv/bin/activate  # o `venv\Scripts\activate` en Windows
    pip install -r requirements.txt
    
    # 3. ¡A analizar!
    python main.py                    # Modo automático
    streamlit run app.py              # Modo interactivo
```

## 🧪 Casos de Uso Reales

- 📌 Para Data Analysts
    - "Necesito entender rápidamente un dataset nuevo antes de presentarlo al cliente"

- 📌 Para Data Scientists
    - "Quiero validar mis supuestos sobre outliers y distribuciones antes de modelar"

- 📌 Para Estudiantes
    - "Estoy aprendiendo análisis de datos y quiero experimentar sin código"

- 📌 Para Product Managers
    - "Necesito explorar métricas de producto sin molestar al equipo de datos"

## 🎯 Próximos Pasos (Roadmap)

- **Más métodos de outliers**: Z-score, MAD, percentiles
- **Soporte para más formatos**: Excel, Parquet, JSON
- **Exportar a PDF**: Reportes listos para compartir
- **Temas personalizables**: Dark/Light mode, colores corporativos

## 👥 Contribuir

¿Tienes una idea? ¿Encontraste un bug? ¡Este proyecto vive de la comunidad!

1. Fork el repo
2. Crea tu branch (git checkout -b feature/lo-increible)
3. Commit (git commit -m 'Add: algo increíble')
4. Push (git push origin feature/lo-increible)
5. Abre un Pull Request
