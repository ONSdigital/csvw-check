package csvwcheck.models

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import csvwcheck.ConfiguredObjectMapper.objectMapper
import org.scalatest.funsuite.AnyFunSuite

class TableGroupTest extends AnyFunSuite {
  test("should create table group object from pre parsed metadata") {
    val json =
      """
        |{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |  "tables": [{
        |    "url": "http://w3c.github.io/csvw/tests/countries.csv",
        |    "tableSchema": {
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
        |        "datatype": { "@id": "number" }
        |      }, {
        |        "name": "name",
        |        "titles": {"en": ["name"] },
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" }
        |      }],
        |      "aboutUrl": "http://example.org/countries.csv{#countryCode}",
        |      "propertyUrl": "http://schema.org/{_name}",
        |      "primaryKey": "countryCode"
        |    }
        |  }, {
        |    "url": "http://w3c.github.io/csvw/tests/country_slice.csv",
        |    "tableSchema": {
        |      "columns": [{
        |        "name": "countryRef",
        |        "titles": {"en": ["countryRef"] },
        |        "valueUrl": "http://example.org/countries.csv{#countryRef}"
        |      }, {
        |        "name": "year",
        |        "titles": {"en": ["year"] },
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#gYear" }
        |      }, {
        |        "name": "population",
        |        "titles": {"en": ["population"] },
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
        |}
        |""".stripMargin
    val jsonObjectNode = objectMapper.readTree(json).asInstanceOf[ObjectNode]
    val Right(WithWarningsAndErrors(tableGroup, _)) = TableGroup.fromJson(jsonObjectNode)
    assert(tableGroup.id.isEmpty)
    assert(tableGroup.tables.size === 2)
    val referencedTable =
      tableGroup.tables("http://w3c.github.io/csvw/tests/countries.csv")
    assert(referencedTable.foreignKeyReferences.length === 1)
    val foreignKeyReference = referencedTable.foreignKeyReferences(0)
    assert(
      foreignKeyReference.referencedTable.url === "http://w3c.github.io/csvw/tests/countries.csv"
    )

    assert(foreignKeyReference.referencedTableReferencedColumns.length === 1)
    assert(
      foreignKeyReference
        .referencedTableReferencedColumns(0)
        .name
        .get === "countryCode"
    )
    assert(foreignKeyReference.foreignKeyDefinition.localColumns.length === 1)
    assert(
      foreignKeyReference.foreignKeyDefinition
        .localColumns(0)
        .name
        .get === "countryRef"
    )
  }

  test(
    "should set foreign key references correctly when using schemaReference instead of resource"
  ) {
    val json =
      """
        |{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://example.com/2/"}],
        |  "tables": [{
        |    "url": "http://w3c.github.io/csvw/tests/countries.csv",
        |    "tableSchema": {
        |      "@id": "http://w3c.github.io/csvw/tests/countries.json",
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
        |      "primaryKey": ["countryCode"]
        |    }
        |  }, {
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
        |        "name": "population",
        |        "titles": {"en": ["population"]},
        |        "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#integer" }
        |      }],
        |      "foreignKeys": [{
        |        "columnReference": ["countryRef"],
        |        "reference": {
        |          "schemaReference": "http://w3c.github.io/csvw/tests/countries.json",
        |          "columnReference": ["countryCode"]
        |        }
        |      }]
        |    }
        |  }]
        |}
        |""".stripMargin
    val jsonObjectNode = objectMapper.readTree(json).asInstanceOf[ObjectNode]
    val Right(WithWarningsAndErrors(tableGroup, _)) = TableGroup.fromJson(
      jsonObjectNode
    )
    val referencedTable =
      tableGroup.tables("http://w3c.github.io/csvw/tests/countries.csv")

    assert(referencedTable.foreignKeyReferences.length === 1)
    val foreignKeyReference = referencedTable.foreignKeyReferences(0)
    assert(foreignKeyReference.referencedTable.url === referencedTable.url)
    assert(foreignKeyReference.referencedTableReferencedColumns.length === 1)
    assert(
      foreignKeyReference
        .referencedTableReferencedColumns(0)
        .name
        .get === "countryCode"
    )
    assert(foreignKeyReference.foreignKeyDefinition.localColumns.length === 1)
    assert(
      foreignKeyReference.foreignKeyDefinition
        .localColumns(0)
        .name
        .get === "countryRef"
    )

  }
  test("should inherit null to all columns") {
    val json =
      """
        |{
        |  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |  "null": ["-"],
        |  "tables": [{
        |    "url": "http://w3c.github.io/csvw/tests/test040.csv",
        |    "tableSchema": {
        |      "columns": [{
        |        "titles": {"en": ["null"]}
        |      }, {
        |        "titles": {"en": ["lang"]}
        |      }, {
        |        "titles": {"en": ["textDirection"]}
        |      }, {
        |        "titles": {"en": ["separator"]}
        |      }, {
        |        "titles": {"en": ["ordered"]}
        |      }, {
        |        "titles": {"en": ["default"]}
        |      }, {
        |        "titles": {"en": ["datatype"]}
        |      }, {
        |        "titles": {"en": ["aboutUrl"]}
        |      }, {
        |        "titles": {"en": ["propertyUrl"]}
        |      }, {
        |        "titles": {"en": ["valueUrl"]}
        |      }]
        |    }
        |  }]
        |}
        |""".stripMargin
    val jsonObjectNode = objectMapper.readTree(json).asInstanceOf[ObjectNode]
    val Right(WithWarningsAndErrors(tableGroup, warningsAndErrors)) =
      TableGroup.fromJson(
        jsonObjectNode
      )
    val table =
      tableGroup.tables("http://w3c.github.io/csvw/tests/test040.csv")

    assert(warningsAndErrors.warnings.length === 0)
    assert(tableGroup.tables.size === 1)
    assert(table.schema.get.columns.length === 10)
    assert(
      table.schema.get.columns(0).nullParam === Array("-")
    ) // should inherit null to all columns - assertion which justifies test name
  }

  test(
    "should initialize TableGroup object correctly when tables key is not present (just one table)"
  ) {
    val json =
      """
        |{
        |    "@context": ["http://www.w3.org/ns/csvw", {"@language": "en", "@base": "http://w3c.github.io/csvw/tests/"}],
        |    "tables": [
        |     {
        |         "url": "http://w3c.github.io/csvw/tests/tree-ops.csv",
        |         "dc:title": "Tree Operations",
        |         "dcat:keyword": ["tree", "street", "maintenance"],
        |         "dc:publisher": {
        |           "schema:name": "Example Municipality",
        |           "schema:url": {"@id": "http://example.org"}
        |         },
        |         "dc:license": {"@id": "http://opendefinition.org/licenses/cc-by/"},
        |         "dc:modified": {"@value": "2010-12-31", "@type": "xsd:date"},
        |         "tableSchema": {
        |           "columns": [{
        |             "name": "GID",
        |             "titles": {"en": ["GID", "Generic Identifier"]},
        |             "dc:description": "An identifier for the operation on a tree.",
        |             "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" },
        |             "required": true
        |           }, {
        |             "name": "on_street",
        |             "titles": {"en": ["On Street"]},
        |             "dc:description": "The street that the tree is on.",
        |             "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" }
        |           }, {
        |             "name": "species",
        |             "titles": {"en": ["Species"]},
        |             "dc:description": "The species of the tree.",
        |             "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" }
        |           }, {
        |             "name": "trim_cycle",
        |             "titles": {"en": ["Trim Cycle"]},
        |             "dc:description": "The operation performed on the tree.",
        |             "datatype": { "@id": "http://www.w3.org/2001/XMLSchema#string" }
        |           }, {
        |             "name": "inventory_date",
        |             "titles": {"en": ["Inventory Date"]},
        |             "dc:description": "The date of the operation that was performed.",
        |             "datatype": {"base": "date", "format": "M/d/yyyy"}
        |           }],
        |           "primaryKey": ["GID"],
        |           "aboutUrl": "#gid-{GID}"
        |         }
        |     }
        |    ]
        |  }
        |""".stripMargin
    val jsonObjectNode = objectMapper.readTree(json).asInstanceOf[ObjectNode]
    val Right(WithWarningsAndErrors(tableGroup, _)) = TableGroup.fromJson(
      jsonObjectNode
    )
    val table =
      tableGroup.tables("http://w3c.github.io/csvw/tests/tree-ops.csv")

    assert(tableGroup.tables.size === 1)
    assert(table.schema.get.columns.length === 5)
  }

  test("ensure baseUrl and lang can be correctly extracted from @context") {

    val json =
      """
        |{
        | "@context": [ "http://www.w3.org/ns/csvw", {
        |   "@language": "fr",
        |   "@base": "http://new-base-url"
        | }]
        |}
        |""".stripMargin
    val objectNode = objectMapper
      .readTree(json)
      .asInstanceOf[ObjectNode]
    val (newBaseUrl, newLang) = TableGroup.parseContext(objectNode)
    assert(newBaseUrl === "http://new-base-url")
    assert(newLang === "fr")
  }

}
