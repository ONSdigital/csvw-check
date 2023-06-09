package csvwcheck.normalisation

import scala.util.matching.Regex

object RegExpressions {
  val startsWithUnderscore: Regex = "^_:.*$".r
  val containsColon: Regex = ".*:.*".r
  val Bcp47Language: String =
    "(?<language>([A-Za-z]{2,3}(-" + Bcp47Extlang + ")?)|[A-Za-z]{4}|[A-Za-z]{5,8})"
  val prefixedPropertyPattern: Regex = "^[a-z]+:.*$".r
  val NameRegExp =
    "^([A-Za-z0-9]|(%[A-F0-9][A-F0-9]))([A-Za-z0-9_]|(%[A-F0-9][A-F0-9]))*$".r
  private val Bcp47Regular =
    "(art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang)"
  private val Bcp47Irregular =
    "(en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)"
  private val Bcp47Grandfathered =
    "(?<grandfathered>" + Bcp47Irregular + "|" + Bcp47Regular + ")"
  private val Bcp47PrivateUse = "(x(-[A-Za-z0-9]{1,8})+)"
  private val Bcp47Singleton = "[0-9A-WY-Za-wy-z]"
  private val Bcp47Extension =
    "(?<extension>" + Bcp47Singleton + "(-[A-Za-z0-9]{2,8})+)"
  private val Bcp47Variant = "(?<variant>[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3})"
  private val Bcp47Region = "(?<region>[A-Za-z]{2}|[0-9]{3})"
  private val Bcp47Script = "(?<script>[A-Za-z]{4})"
  private val Bcp47Extlang = "(?<extlang>[A-Za-z]{3}(-[A-Za-z]{3}){0,2})"
  private val Bcp47Langtag =
    "(" + Bcp47Language + "(-" + Bcp47Script + ")?" + "(-" + Bcp47Region + ")?" + "(-" + Bcp47Variant + ")*" + "(-" + Bcp47Extension + ")*" + "(-" + Bcp47PrivateUse + ")?" + ")"
  val Bcp47LanguageTagRegExp: Regex =
    ("^(" + Bcp47Grandfathered + "|" + Bcp47Langtag + "|" + Bcp47PrivateUse + ")").r
}
