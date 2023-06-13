package csvwcheck.models

import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.errors.MetadataError
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import org.scalatest.funsuite.AnyFunSuite

//noinspection HttpUrlsUsage
class TableTest extends AnyFunSuite {
  test("should create a table from pre-parsed CSV-W metadata") {
    val json =
      """{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |  "tables": [{
        |    "@id": "http://w3c.github.io/csvw/tests/sample_id_value",
        |    "dialect": {"encoding": "utf-8"},
        |    "notes": ["sample value"],
        |    "url": "http://w3c.github.io/csvw/tests/countries.csv",
        |    "suppressOutput": true,
        |    "@type": "Table",
        |    "tableSchema": {
        |      "@id": "http://w3c.github.io/csvw/tests/sample_id_value",
        |      "columns": [{
        |        "name": "countryCode",
        |        "titles": {"en": ["countryCode"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" },
        |        "propertyUrl": "http://www.geonames.org/ontology{#_name}"
        |      }, {
        |        "name": "latitude",
        |        "titles": {"en": ["latitude"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#number" }
        |      }, {
        |        "name": "longitude",
        |        "titles": {"en": ["longitude"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#number" }
        |      }, {
        |        "name": "name",
        |        "titles": {"en": ["name"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" }
        |      }],
        |      "aboutUrl": "http://example.org/countries.csv{#countryCode}",
        |      "propertyUrl": "http://schema.org/{_name}",
        |      "primaryKey": ["countryCode"],
        |      "rowTitles": ["countryCode"]
        |    }
        |  }, {
        |    "url": "http://w3c.github.io/csvw/tests/country_slice.csv",
        |    "@type": "Table",
        |    "tableSchema": {
        |      "columns": [{
        |        "name": "countryRef",
        |        "titles": {"en": ["countryRef"]},
        |        "valueUrl": "http://example.org/countries.csv{#countryRef}"
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["year"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#gYear" }
        |      }, {
        |        "name": "population",
        |        "titles": {"en": ["population"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#integer" }
        |      }],
        |      "primaryKey": ["population"],
        |      "foreignKeys": [{
        |        "columnReference": ["countryRef"],
        |        "reference": {
        |          "resource": "http://w3c.github.io/csvw/tests/countries.csv",
        |          "columnReference": ["countryCode"]
        |        }
        |      }]
        |    }
        |  }]
        |}""".stripMargin
    val jsonNode = objectMapper.readTree(json)
    val tableObject1 = jsonNode.get("tables").elements().asScalaArray(0)
    val tableObject2 = jsonNode.get("tables").elements().asScalaArray(1)
    val Right((table1, w1)) = Table.fromJson(tableObject1.asInstanceOf[ObjectNode])

    val Right((table2, _)) = Table.fromJson(tableObject2.asInstanceOf[ObjectNode])

    assert(table1.url === "http://w3c.github.io/csvw/tests/countries.csv")
    assert(table1.id.get === "http://w3c.github.io/csvw/tests/sample_id_value")
    assert(table1.schema.get.columns.length === 4)
    assert(table1.dialect.get.encoding === "utf-8")
    assert(table2.schema.get.foreignKeys.length === 1)
    assert(
      table2.schema.get.foreignKeys(0).localColumns(0).name.get === "countryRef"
    )
    assert(table1.notes.isDefined)
    assert(
      table1.notes.get.elements().asScalaArray(0) === new TextNode(
        "sample value"
      )
    )
    assert(table1.schema.exists(_.primaryKey(0).name.contains("countryCode")))
    assert(table1.schema.exists(_.rowTitleColumns.length === 1))
    assert(table1.schema.exists(_.rowTitleColumns(0).name.contains("countryCode")))
    assert(
      table1.schema.exists(_.schemaId.contains("http://w3c.github.io/csvw/tests/sample_id_value"))
    )
    assert(table1.suppressOutput === true)
    assert(w1.isEmpty)
  }

  test("should return error for duplicate column names") {
    val json =
      """{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |  "tables": [{
        |    "url": "http://w3c.github.io/csvw/tests/country_slice.csv",
        |    "tableSchema": {
        |      "columns": [{
        |        "name": "countryRef",
        |        "titles": {"en": ["countryRef"]},
        |        "valueUrl": "http://example.org/countries.csv{#countryRef}"
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["year"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#gYear" }
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["population"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#integer" }
        |      }],
        |      "foreignKeys": [{
        |        "columnReference": ["countryRef"],
        |        "reference": {
        |          "resource": "http://w3c.github.io/csvw/tests/countries.csv",
        |          "columnReference": ["countryCode"]
        |        }
        |      }]
        |    }
        |  }]
        |}""".stripMargin
    val jsonNode = objectMapper.readTree(json)
    val Left(metadataError) = Table.fromJson(jsonNode.get("tables").elements().next().asInstanceOf[ObjectNode])
    assert(metadataError.message == "Multiple columns named year")
  }

  test(
    "should return error if virtual columns are found before non virtual columns"
  ) {
    val json =
      """{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |  "tables": [{
        |    "url": "http://w3c.github.io/csvw/tests/country_slice.csv",
        |    "tableSchema": {
        |      "columns": [{
        |        "name": "countryRef",
        |        "titles": {"en": ["countryRef"]},
        |        "valueUrl": "http://example.org/countries.csv{#countryRef}"
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["year"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#gYear" },
        |        "virtual": true
        |      }, {
        |        "name": "population",
        |        "titles": {"en": ["population"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#integer" }
        |      }],
        |      "foreignKeys": [{
        |        "columnReference": ["countryRef"],
        |        "reference": {
        |          "resource": "http://w3c.github.io/csvw/tests/countries.csv",
        |          "columnReference": ["countryCode"]
        |        }
        |      }]
        |    }
        |  }]
        |}""".stripMargin
    val jsonNode = objectMapper.readTree(json)
    val Left(metadataError) = Table.fromJson(jsonNode.get("tables").elements().next().asInstanceOf[ObjectNode])
    assert(
      metadataError.message == "virtual columns before non-virtual column population (3)"
    )
  }

  test("should return error if url is not present for table") {
    val json =
      """{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |  "tables": [{
        |    "tableSchema": {
        |      "columns": [{
        |        "name": "countryRef",
        |        "titles": {"en": ["countryRef"]},
        |        "valueUrl": "http://example.org/countries.csv{#countryRef}"
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["year"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#gYear" }
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["population"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#integer" }
        |      }],
        |      "foreignKeys": [{
        |        "columnReference": ["countryRef"],
        |        "reference": {
        |          "resource": "http://w3c.github.io/csvw/tests/countries.csv",
        |          "columnReference": ["countryCode"]
        |        }
        |      }]
        |    }
        |  }]
        |}""".stripMargin
    val jsonNode = objectMapper.readTree(json)
    val Left(metadataError) = Table.fromJson(jsonNode.get("tables").elements().next().asInstanceOf[ObjectNode])

    assert(metadataError.message == "URL not found for table")
  }
}
