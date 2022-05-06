# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import plotly.graph_objects as graph_objects
import findspark

findspark.init()

# spark_context = SparkContext.getOrCreate()
spark_context = SparkContext('local', 'R1_A2')
spark_session = SparkSession(spark_context)
sqlContext = SQLContext(spark_context)

# %%
text_file = sqlContext.read.format('com.databricks.spark.csv').\
    options(header='true', inferschema='true', quote='"', delimiter=',').\
    load('./work/data/TraficoAereoGt.csv')

rddfiltro = text_file.rdd.map(tuple)
rddGen = rddfiltro.map(lambda word: (word[2], word[3]))
# rddGen.take(10)

# %%
rddConteo = rddGen.reduceByKey(lambda a, b: a+b)
print(f'Conteo total -> {rddConteo.collect()}')

# %%
rddNombres = rddConteo.map(lambda x: (x[0]))
print(rddNombres.collect())

rddTotales = rddConteo.map(lambda x: (x[1]))
print(rddTotales.collect())

# %%
graph = graph_objects.Figure(
    data=graph_objects.Bar(
        x=rddNombres.collect(),
        y=rddTotales.collect()
    ))

graph.update_layout(
    title_text='Total de aterrizajes por Aeropuerto',
    title_font_size=30,
    yaxis=dict(title='No. Aterrizajes', title_font_size=25),
    xaxis=dict(title='Aeropuerto', title_font_size=25))

graph.update_traces(overwrite=True, marker={"opacity": 0.5})
graph.write_html('./work/reports/R1_A2.html', auto_open=True)
