# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import plotly.graph_objects as graph_objects
import findspark

findspark.init()

# spark_context = SparkContext.getOrCreate()
spark_context = SparkContext('local', 'First App')
spark_session = SparkSession(spark_context)
sqlContext = SQLContext(spark_context)

# %%
text_file = sqlContext.read.format('com.databricks.spark.csv').\
    options(header='true', inferschema='true', quote='"', delimiter=',').\
    load('./work/data/suicide.csv')

rddfiltro = text_file.rdd.map(tuple)
rddGen = rddfiltro.map(lambda word: (word[8], word[4]))
rddGen.take(10)

# %%
rddConteo = rddGen.reduceByKey(lambda a, b: a+b)
print(f'Conteo total -> {rddConteo.collect()}')

rddOrden = spark_context.parallelize(
    rddConteo.sortBy(lambda a: a[1], True).take(3))
print(f'3 generaciones con menor suicidios -> {rddOrden.collect()}')

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
    title_text='Generaciones con el menor numero de suicidios',
    title_font_size=30)

graph.update_traces(
    hoverinfo='label+percent',
    textinfo='value',
    textfont_size=20)

graph.write_html('./work/reports/GraficoPie.html', auto_open=True)
