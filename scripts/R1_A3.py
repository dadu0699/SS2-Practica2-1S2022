# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import plotly.graph_objects as graph_objects
import findspark

findspark.init()

# spark_context = SparkContext.getOrCreate()
spark_context = SparkContext('local', 'R1_A3')
spark_session = SparkSession(spark_context)
sqlContext = SQLContext(spark_context)

# %%
text_file = sqlContext.read.format('com.databricks.spark.csv').\
    options(header='true', inferschema='true', quote='"', delimiter=',').\
    load('./work/data/Covid19.csv')

rddfiltro = text_file.rdd.map(tuple)
rddGen = rddfiltro.map(lambda word: (word[7], word[5]))
# rddGen.take(10)

# %%
rddConteo = rddGen.reduceByKey(lambda a, b: a+b)
print(f'Conteo total -> {rddConteo.collect()}')

# %%
filtros = ['Cuba', 'France', 'Canada', 'Singapore', 'South_Korea']
rddOrden = spark_context.parallelize(
    rddConteo.filter(lambda a: a[0] in filtros).take(5))
print(f'Total de casos de COVID-19 en {filtros} -> {rddOrden.collect()}')

# %%
rddNombres = rddOrden.map(lambda x: (x[0]))
print(rddNombres.collect())

rddTotales = rddOrden.map(lambda x: (x[1]))
print(rddTotales.collect())

# %%
graph = graph_objects.Figure(
    data=graph_objects.Bar(
        x=rddNombres.collect(),
        y=rddTotales.collect()
    ))

graph.update_layout(
    title_text='País con el mayor valor por exportaciones',
    title_font_size=30,
    yaxis=dict(title='No. Exportaciones', title_font_size=25),
    xaxis=dict(title='País', title_font_size=25))

graph.update_traces(overwrite=True, marker={"opacity": 0.5})
graph.write_html('./work/reports/R1_A3.html', auto_open=True)
