package semantix.test.commons.data

import java.sql.{Date, ResultSet, Timestamp}
import java.util.UUID

import semantix.test.commons.handlers.spark.Spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import semantix.test.commons.handlers.spark.Spark



class HTTPNasa extends Serializable {
  val ss: SparkSession = Spark.getSession

  Spark.setLogLevel(Level.WARN)

  import ss.implicits._
  //val textFile = RDD(Text.TextFile)
  //val  = RDD(teste)

  val sc = Spark. getContext
  val textFile = sc.textFile ("[*]C:\\Users\\wesley.de.britto\\Desktop\\access_log_Jul95")
  val textFile1 = sc.textFile ("C:\\Users\\wesley.de.britto\\Desktop\\access_log_Aug95")



  // 1. Número de hosts únicos.

  //Trago a primeira coluna e com contador tirando os espacos dos campos
  val colHosts = textFile.map(_.split(" ")).map(column=>(column(0),1))

  //tirando duplicadas
  val hostsCounted = colHosts.reduceByKey(_ + _)

  //traz o numero de host unico, no caso o primeiro que for = 1
  val uniqueHosts = hostsCounted.filter(_._2 == 1)

  // 2. O total de erros 404.

  //apenas as linhas que contenham "404"
  val erro404 = textFile.map(_.split(" ")).filter(_.contains("404"))

  // Separo o numero de colunas com filter e pego todos que contenham 404
  val colStatus = erro404.map(column=>(column(8),1)).filter(_._1 == "404")

  //o numero de linhas com o resultado
  val countStatus404 = colStatus.count()

  // 3. Os 5 URLs que mais causaram erro 404.

  // Traz os campos da requisicao + status "404"
  val reqMaisStatus = erro404.map(column=>(column(5) + " " + column(6) + " " + column(7),column(8),1))

  //trazer apenas os campos com status 404
  val requestStatus404 = reqMaisStatus.filter(_._2 == "404")

  //Mapeamento para realizar contagem
  var request404 = requestStatus404.map({case (a,b,c)=>((a,b),c)})

  //Reduzo todos os casos repetidos
  request404 = request404.reduceByKey(_ + _)

  //Ordenar de forma decrescente todos os resultados para tomar aqueles com maior contagem
  request404.sortBy(_._2).take(5).foreach(println)

  // 4. Quantidade de erros 404 por dia.

  // linhas que contenham "404" e filtro para pegar o dias desejados
  val timestampMaisStatus = erro404.map(column=>(column(3).substring(1,12),column(8),1))

  //filtro todos os campo que contenham 404
  val dateStatus404 = timestampMaisStatus.filter(_._2 == "404")

  //Mapeamento para realizar contagem
  var dateStatus404map = dateStatus404.map({case (a,b,c)=>((a,b),c)})

  //tira todos os campos repetidos
  dateStatus404map = dateStatus404map.reduceByKey(_ + _)


  // 5. O total de bytes retornados.
  val validLines = textFile.map(_.split(" ")).filter{_.size == 10}

  val bytes = validLines.map(column=>(column(8), column(9)))
  val bytesValidos = bytes.filter(_._2 != "-").map({case (a,b) => b.toInt})

  //Somar todos os valores

  val bytesSum = bytesValidos.reduce(_ + _)

}