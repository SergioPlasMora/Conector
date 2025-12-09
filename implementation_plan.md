# **Especificación del Sistema de Comunicación de Datos Distribuidos**

**Proyecto:** SaaS de Analítica Descentralizada

**Versión:** 1.5 (Corrección: Datos Estáticos)

**Fecha:** 08 de Diciembre, 2025

## **1\. Contexto del Proyecto**

El objetivo es desarrollar la infraestructura de comunicación para una plataforma **SaaS de Analítica**. A diferencia de los modelos tradicionales donde los datos se centralizan en la nube, esta plataforma opera bajo un modelo descentralizado: **los datos residen en la infraestructura del cliente (On-Premise)**.

Los DataSets (conjuntos de datos) deben ser extraídos bajo demanda desde los nodos locales y entregados a las interfaces de consumo (Aplicaciones Web SPA, Apps Móviles, o integraciones API) para su visualización y análisis en tiempo real.

### **Reto Principal**

Lograr una transferencia de datos segura, eficiente y transparente entre redes privadas (Nodos) y consumidores públicos (Aplicaciones), sin exponer puertos en la infraestructura del cliente y adaptándose a volúmenes de datos variables (desde 1KB hasta 100MB+).

## **2\. Definición de Componentes de Software**

Todos los componentes se desarrollarán utilizando el ecosistema **Python**.

| Componente | Tipo | Implementación | Descripción Funcional |
| :---- | :---- | :---- | :---- |
| **1\. Nodo** | Hardware | N/A | Dispositivo físico (computadora/servidor) ubicado *on-premise* en la red del cliente. Tiene conexión de salida a Internet, pero no puertos de entrada abiertos. |
| **2\. Conector** | Software | **Python (CLI App)** | Aplicación de línea de comandos que se ejecuta en el Nodo. Inicia y mantiene el canal con el Enrutador. Debe ser capaz de recibir múltiples solicitudes concurrentes. |
| **3\. Enrutador** | Software | **Python (FastAPI)** | Aplicación central asíncrona. Orquesta el tráfico, verifica estados y enruta flujos de datos entre la Aplicación y el Conector. |
| **4\. Servidor** | Hardware | N/A | Infraestructura Cloud que aloja y ejecuta al **Enrutador**. |
| **5\. Aplicación** | Software | **Python (CLI App)** | El componente consumidor (simulando una SPA o App Móvil) que consume la API del Enrutador para solicitar DataSets. |
| **6\. DataSet** | Datos | Binario/Texto | El conjunto de información objeto de la transferencia. |

## **3\. Principios de Implementación de Prototipos**

Para la fase de "Laboratorio de Arquitectura", el desarrollo se regirá por los siguientes principios estrictos:

### **3.1. Desarrollo Minimalista (KISS)**

* **Código Esencial:** Se debe programar **únicamente** lo necesario para validar el patrón de diseño específico bajo prueba.  
* **Cero Autenticación:** Para esta fase de pruebas, **no se implementará ningún mecanismo de autenticación ni autorización**. El sistema será totalmente abierto: cualquier instancia de la **Aplicación** podrá solicitar datos a cualquier **Nodo** conectado sin restricciones de seguridad ni tokens.  
* **Sin "Gold Plating":** Evitar validaciones de negocio complejas o interfaces gráficas. El foco es puramente la mecánica de transporte de datos.

### **3.2. Observabilidad y Métricas (Built-in)**

Cada componente debe incluir instrumentación nativa para medir el desempeño sin necesidad de herramientas externas complejas:

* **Traceability IDs:** Cada solicitud generada por la Aplicación debe incluir un request\_id único que se propague por el Enrutador y el Conector, y regrese en la respuesta.  
* **Timestamps:** Registrar tiempos de alta precisión (nanosegundos) en puntos clave:  
  * t0: Aplicación envía solicitud.  
  * t1: Enrutador recibe solicitud.  
  * t2: Conector recibe solicitud.  
  * t3: Conector inicia envío de datos.  
  * t4: Aplicación termina de recibir datos.  
* **Logs Estructurados:** Salida de logs en formato parseable (JSON/CSV) para facilitar el análisis posterior.

### **3.3. Configuración Dinámica**

Cada componente (Aplicación, Enrutador, Conector) debe leer un archivo de configuración (ej. config.yaml o .env) al iniciar para modificar su comportamiento sin cambiar código:

* Timeouts y reintentos.  
* Tamaños de buffer (chunk sizes).  
* Simulación de latencia artificial.  
* Parámetros de conexión.

## **4\. Estrategia de Simulación y Carga**

Para validar la arquitectura sin depender de datos reales o infraestructura de producción, se implementarán mecanismos de emulación:

### **4.1. Emulación en el Conector**

El Conector no consultará bases de datos reales ni generará datos sintéticos al vuelo.

* **Lectura de Datos Estáticos:** El Conector **utilizará únicamente archivos pre-generados** alojados localmente. Su responsabilidad se limita a ubicar el archivo solicitado por la Aplicación (ej. dataset\_100MB.parquet, dataset\_1kb.json) y leerlo del disco.  
* **Simulación de Latencia (Processing Time):** Configurable mediante un parámetro processing\_delay. Antes de leer el archivo, el Conector "dormirá" (sleep) este tiempo para emular el costo computacional de generar una consulta SQL compleja en un escenario real.  
* **Concurrencia Local:** El Conector debe implementarse usando hilos (threading) o asincronía (asyncio) para aceptar nuevas solicitudes del Enrutador *mientras* está ocupado leyendo o transmitiendo un DataSet previo.

### **4.2. Emulación de Escenario Masivo (Load Testing)**

Para estresar el Enrutador y medir su comportamiento con múltiples Nodos y Aplicaciones:

* **Aplicaciones Virtuales:** Se utilizará un script orquestador (o herramientas como Locust modificado) que instancie múltiples procesos Aplicación CLI concurrentes.  
* **Nodos Virtuales:** Se desplegarán múltiples instancias del Conector CLI (posiblemente en contenedores Docker o procesos ligeros) conectándose al mismo Enrutador para simular una flota de Nodos distribuidos.

## **5\. Protocolo Base de Comunicación**

El sistema opera bajo un modelo síncrono de solicitud-respuesta orquestado por el Enrutador.

### **Flujo de la Transacción**

1. **Canal Abierto:** El **Conector** inicia una conexión saliente hacia el **Enrutador** y mantiene el canal activo.  
2. **Solicitud:** La **Aplicación** realiza una petición HTTP/API al **Enrutador** solicitando un DataSet específico de un Nodo.  
3. **Validación y Enrutamiento:** El **Enrutador** valida la conexión del Nodo y reenvía la solicitud.  
4. **Procesamiento Simulado:** El **Conector** recibe la orden, localiza el archivo solicitado en disco y espera el tiempo de processing\_delay configurado (simulando la generación).  
5. **Transferencia:** El **Conector** lee el archivo y envía los datos al **Enrutador** según el patrón de diseño activo.  
6. **Entrega Final:** El **Enrutador** entrega los datos a la **Aplicación**.

## **6\. Patrones de Diseño Iniciales (Candidatos)**

Se evaluarán los siguientes tres patrones arquitectónicos:

### **Patrón A: Proxy Directo (Buffering)**

* **Mecanismo:** Enrutador carga todo el DataSet en RAM antes de enviar.  
* **Foco de Prueba:** Límite de memoria del Enrutador y simplicidad.

### **Patrón B: Streaming Proxy (Pasamanos)**

* **Mecanismo:** Enrutador reenvía bytes (chunks) inmediatamente a la Aplicación.  
* **Foco de Prueba:** Latencia (TTFB) y manejo de conexiones largas.

### **Patrón C: Almacenamiento Intermedio (Offloading)**

* **Mecanismo:** Conector sube datos a almacenamiento temporal (MinIO/Redis); Enrutador entrega enlace a la Aplicación.  
* **Foco de Prueba:** Throughput en archivos grandes y liberación de recursos del Enrutador.

## **7\. Matriz de Evaluación**

Cada prueba debe registrar y reportar:

| Métrica | Definición | Objetivo |
| :---- | :---- | :---- |
| **TTFB (Time to First Byte)** | t\_primer\_byte\_aplicacion \- t0. | Evaluar latencia percibida. |
| **Throughput Total** | Tamaño DataSet / (t4 \- t0). | Evaluar velocidad efectiva. |
| **Overhead de Protocolo** | Tiempo añadido por el Enrutador (t\_total \- t\_transmision\_pura). | Identificar cuellos de botella en el Enrutador. |
| **Recursos Servidor** | CPU/RAM del proceso Enrutador. | Dimensionamiento de infraestructura. |
| **Estabilidad** | % de solicitudes exitosas bajo carga concurrente (N aplicaciones x M conectores). | Validar robustez. |