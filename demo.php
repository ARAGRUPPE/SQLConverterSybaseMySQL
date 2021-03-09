<?php
error_reporting(E_ALL ^ E_NOTICE);
ini_set("display_errors", true);
ini_set('xdebug.var_display_max_data', 1024 * 1024);

$sqlSybase = "select convert(char(10), getdate(), 104) heute,
                     convert(char(10), dateadd(day, 1, getdate()), 104) morgen,
                     convert(char(10), dateadd(day, -1, getdate()), 104) gestern,
                     datepart(quarter, getdate()) quartal,
                     convert(char(10), dateadd(month, -1, getdate()), 104) vor_einem_monat";

require_once("SQLConverterSybaseMySQL.class.php");

$s = new SQLConverterSybaseMySQL($sqlSybase);
$sqlMySQL = $s->convert();

var_dump($sqlSybase);
var_dump($sqlMySQL);