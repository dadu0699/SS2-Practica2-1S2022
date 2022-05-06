# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import plotly.graph_objects as graph_objects
import findspark

findspark.init()

# spark_context = SparkContext.getOrCreate()
spark_context = SparkContext('local', 'R3_A3')
spark_session = SparkSession(spark_context)
sqlContext = SQLContext(spark_context)

# %%
text_file = sqlContext.read.format('com.databricks.spark.csv').\
    options(header='true', inferschema='true', quote='"', delimiter=',').\
    load('./work/data/Covid19.csv')

rddfiltro = text_file.rdd.map(tuple)
rddGen = rddfiltro.filter(lambda word: word[3] in ['August'])
rddGen = rddGen.map(lambda word: (word[7], word[5] + word[6]))
# rddGen.take(10)

# %%
rddConteo = rddGen.reduceByKey(lambda a, b: a+b)
print(f'Conteo total -> {rddConteo.collect()}')

# %%
filtros = ['Guatemala']
rddOrden = spark_context.parallelize(
    rddConteo.filter(lambda a: a[0] in filtros).take(1))
print(f'Total de casos de COVID-19 en {filtros} -> {rddOrden.collect()}')

# %%
rddNombres = rddOrden.map(lambda x: (x[0]))
print(rddNombres.collect())

rddTotales = rddOrden.map(lambda x: (x[1]))
print(rddTotales.collect())

# %%
graph = graph_objects.Figure(
    data=graph_objects.Pie(
        labels=rddNombres.collect(),
        values=rddTotales.collect()
    ))

graph.update_layout(
    title_text='Total de casos y muertes en Guatemala en el mes de Agosto',
    title_font_size=30)

graph.update_traces(
    hoverinfo='label+percent',
    textinfo='value',
    textfont_size=20)

graph.write_html('./work/reports/R3_A3.html', auto_open=True)
