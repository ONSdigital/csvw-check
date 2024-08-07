/*
 * Copyright 2020 Crown Copyright (Office for National Statistics)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package csvwcheck

import scala.collection.mutable

//noinspection HttpUrlsUsage
object NameSpaces {
  val values: mutable.HashMap[String, String] = mutable.HashMap(
    "dcat" -> "http://www.w3.org/ns/dcat#",
    "qb" -> "http://purl.org/linked-data/cube#",
    "grddl" -> "http://www.w3.org/2003/g/data-view#",
    "ma" -> "http://www.w3.org/ns/ma-ont#",
    "org" -> "http://www.w3.org/ns/org#",
    "owl" -> "http://www.w3.org/2002/07/owl#",
    "prov" -> "http://www.w3.org/ns/prov#",
    "rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfa" -> "http://www.w3.org/ns/rdfa#",
    "rdfs" -> "http://www.w3.org/2000/01/rdf-schema#",
    "rif" -> "http://www.w3.org/2007/rif#",
    "rr" -> "http://www.w3.org/ns/r2rml#",
    "sd" -> "http://www.w3.org/ns/sparql-service-description#",
    "skos" -> "http://www.w3.org/2004/02/skos/core#",
    "skosxl" -> "http://www.w3.org/2008/05/skos-xl#",
    "wdr" -> "http://www.w3.org/2007/05/powder#",
    "void" -> "http://rdfs.org/ns/void#",
    "wdrs" -> "http://www.w3.org/2007/05/powder-s#",
    "xhv" -> "http://www.w3.org/1999/xhtml/vocab#",
    "xml" -> "http://www.w3.org/XML/1998/namespace",
    "xsd" -> "http://www.w3.org/2001/XMLSchema#",
    "csvw" -> "http://www.w3.org/ns/csvw#",
    "cnt" -> "http://www.w3.org/2008/content",
    "earl" -> "http://www.w3.org/ns/earl#",
    "ht" -> "http://www.w3.org/2006/http#",
    "oa" -> "http://www.w3.org/ns/oa#",
    "ptr" -> "http://www.w3.org/2009/pointers#",
    "cc" -> "http://creativecommons.org/ns#",
    "ctag" -> "http://commontag.org/ns#",
    "dc" -> "http://purl.org/dc/terms/",
    "dcterms" -> "http://purl.org/dc/terms/",
    "dc11" -> "http://purl.org/dc/elements/1.1/",
    "foaf" -> "http://xmlns.com/foaf/0.1/",
    "gr" -> "http://purl.org/goodrelations/v1#",
    "ical" -> "http://www.w3.org/2002/12/cal/icaltzd#",
    "og" -> "http://ogp.me/ns#",
    "rev" -> "http://purl.org/stuff/rev#",
    "sioc" -> "http://rdfs.org/sioc/ns#",
    "v" -> "http://rdf.data-vocabulary.org/#",
    "vcard" -> "http://www.w3.org/2006/vcard/ns#",
    "schema" -> "http://schema.org/"
  )

}
