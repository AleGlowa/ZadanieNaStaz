import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
// Do przetwarzania plików xml
import com.databricks.spark.xml
// Do definiowania własnego schematu danych w plikach xml
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

object Main {
  def main(args: Array[String]): Unit = {
    // Pobierz czas systemowy w nanosekundach
    val startTime = System.nanoTime()

    // Otwórz sesję Spark
    val spark = SparkSession
      .builder()
      .appName("SummaryOfProcurements")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val sqlContext = spark.sqlContext

    // Wczytaj i wyświetl dane
    loadData(sqlContext)

    // Zmierz czas działania programu w sekundach
    val duration = (System.nanoTime() - startTime) / 1e9d
    print(duration + " seconds")
  }

  def loadData(sqlContext: SQLContext): Unit = {
    var df: DataFrame = null
    var newDF: DataFrame = null
    // Maksymalna liczba wierszy do wyświetlenia
    val maxCapCountries = 200

    // Tworzenie własnego schematu danych z plików xml. Ograniczyłem się do wartości, które są potrzebne w zadaniu.
    // Dzięki temu przetwarzanie plików xml jest szybsze
    // Zmiana typu danych dla cen zamówień na typ Decimal, żeby usunąć notację naukową
    val customSchema = StructType(Array(StructField("ISO_COUNTRY", StructType(
                                                                   List(StructField("@VALUE", StringType, nullable = false))), nullable = false),
                                        StructField("VALUES", StructType(
                                                              List(StructField("VALUE", StructType(
                                                                                        List(StructField("@CURRENCY", StringType, nullable = false),
                                                                                             StructField("_VALUE", DecimalType(20, 6), nullable = false)))))), nullable = true)))


    df = sqlContext.read
        .format("xml")
        .option("rowTag", "NOTICE_DATA")
        // Definiowanie przedrostka do atrybutów w plikach xml
        .option("attributePrefix", "@")
        // Do szybkiego wczytywania danych
        .option("samplingRatio", 0.1)
        .schema(customSchema)
        // Wczytanie danych z początku roku 2018 do dziś
        .load("data/2018/2018-*/2018*/*.xml")

    // Utworzenie tymczasowego widoku do późniejszego wykorzystania w zapytaniu SQL
    df.createOrReplaceTempView("Procurements")



    // Zapytanie SQL wybiera: kody krajów, średnią wartość zamówień dla poszczególnych krajów(null - nie podano cen zamówień dla danego kraju w plikach xml),
    // miesieczną średnią liczbę zamówień dla każdego kraju.
    // Zapytanie na moim sprzęcie trwa około 25 minut.
    newDF = sqlContext.sql(
      """ SELECT ISO_COUNTRY['@VALUE'] AS country_code,
        |        ROUND(AVG(VALUES.VALUE['_VALUE']), 2) AS average_price_in_euro,
        |        ROUND(COUNT(*) / 30, 2) AS monthly_average_of_no_procurements
        | FROM Procurements
        | GROUP BY country_code """.stripMargin)


    // Wyświetlenie wyników w tabelii
    newDF.show(maxCapCountries)
  }
}
