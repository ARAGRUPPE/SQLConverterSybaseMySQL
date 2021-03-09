<?php

require_once("ErrorHandler.class.php");

/**
 * Class SQLConverterSybaseMySQL
 * Konvertiert SQL-Statements von Sybase-Syntax auf MySQL-Syntax
 */
class SQLConverterSybaseMySQL
{
    private $stmtOriginal;

    /**
     * SQLConverterSybaseMySQL constructor.
     * @param string $stmt - SQL-Statement, welches auf MySQL konvertiert werden soll
     */
    public function __construct(string $stmt)
    {
        $this->stmtOriginal = $stmt;
    }

    /**
     * Konvertiert das in der Klasse hinterlegte SQL-Statement auf MySQL-Syntax
     * @return string
     */
    public function convert(): string
    {
        // Um nicht alle Konvertierungsschritte durchführen zu müssen, soll anfänglich geprüft werden,
        // ob überhaupt eine Konvertierung erforderlich ist.
        // Eventuell handelt es sich ja schon um ein MySQL-Valides SQL-Statement.
        if (!$this->convertNeeded()) {
            return $this->stmtOriginal;
        }

        // Ok - es muss konvertiert werden.
        $stmtConverted = $this->stmtOriginal;

        // --------------
        // getdate -> now
        // --------------
        $stmtConverted = str_ireplace("getdate", "NOW", $stmtConverted);

        // ----------------
        // isNull -> ifNull
        // ----------------
        $stmtConverted = str_ireplace("isnull", "IFNULL", $stmtConverted);

        // -----------------------------------------------------------
        // Datenbankübergreifende Bezüge
        // stellen_digido_test..stellen -> stellen_digido_test.stellen
        // -----------------------------------------------------------
        preg_match_all('/[a-z_]+\.\.[a-z_]+/i', $stmtConverted, $out);
        foreach ($out as $o) {
            $oNew = str_replace("..", ".", $o);
            $stmtConverted = str_replace($o, $oNew, $stmtConverted);
        }

        // --------------------------
        // top prefix -> limit suffix
        // --------------------------
        preg_match_all('/top\s*(\d+)/i', $stmtConverted, $out);
        foreach ($out[0] as $o) {
            // TOP [d] entfernen
            $stmtConverted = str_replace($o, '', $stmtConverted);
        }
        foreach ($out[1] as $o) {
            // LIMIT [d] hinzufügen
            if ((int)$o > 0) {
                $stmtConverted .= " LIMIT " . (int)$o;
            }
        }

        // ---------------------------------------------------------
        // Funktions-Handling (convert, datepart, datediff, dateadd)
        // ---------------------------------------------------------
        $this->convertFunction("convert", $stmtConverted);
        $this->convertFunction("datepart", $stmtConverted);
        $this->convertFunction("datediff", $stmtConverted);
        $this->convertFunction("dateadd", $stmtConverted);

        // ------------------------------------------------------------------------------
        // Konkatinierung von Strings (kon_vw + ' ' + kon_nr -> concat(kon_vw,' ',kon_nr)
        // ------------------------------------------------------------------------------
        /*
        // Durch diese Konvertierungen werden leider auch mathematische Additionen deaktiviert
        // Die Funktion kann man anhand der Config-Einstellung "ConvertFromSybaseWithConcat" steuern.
        // SQL-Mode PIPES_AS_CONCAT hinzufügen
        DatabaseHandler::addSQLMode("PIPES_AS_CONCAT");
        $stmtConverted = str_ireplace("+", "||", $stmtConverted);
        */

        return $stmtConverted;
    }

    /**
     * Prüft, ob das SQL-Statement für die Verwendung in MySQL konvertiert werden muss
     * @return bool
     */
    private function convertNeeded(): bool
    {
        // Sybase spezifische Ausdrücke definieren
        $needles = array();
        $needles[] = "getdate";
        $needles[] = "isnull";
        $needles[] = "..";
        $needles[] = "top";
        $needles[] = "convert";
        $needles[] = "datepart";
        $needles[] = "datediff";
        $needles[] = "dateadd";
        /*
        if ($this->tryConcatenation) {
            $needles[] = "+";
        }
        */

        foreach ($needles as $n) {
            if (stripos($this->stmtOriginal, $n) !== false) {
                // Vorkommen entdeckt. Konvertierung erforderlich.
                return true;
            }
        }

        // Keine Konvertierung notwendig
        return false;
    }

    /**
     * Konvertiert eine SQL-Funktion von Sybase auf MySQL
     * z.B.: convert/datepart/datediff
     * @param $functionName - convert | datepart | datediff | dateadd
     * @param &$stmtConverted
     */
    private function convertFunction(string $functionName, string &$stmtConverted)
    {
        while (true) {
            $fktStart = stripos($stmtConverted, $functionName);
            if ($fktStart === false) {
                // Funktionsname nicht im SQL gefunden - Nichts zu konvertieren - Ok
                return;
            }

            // Korrespondierendes Funktionsende suchen
            $cnt = $fktStart;
            $cntLevel = 0;
            $fktStartPos = 0;
            $fktEndPos = 0;
            foreach (str_split(substr($stmtConverted, $fktStart)) as $chrPart) {
                $cnt++;
                if ($chrPart === '(') {
                    if ($fktStartPos === 0) {
                        // Funktionsstart gefunden
                        $fktStartPos = $cnt;
                        continue;
                    }
                    $cntLevel++;
                }
                if ($chrPart === ')') {
                    if ($cntLevel === 0) {
                        $fktEndPos = $cnt;
                        break;
                    }
                    $cntLevel--;
                }
            }

            // Es muss ein Funktionsstart/ende gefunden werden - ansonsten stimmt was nicht
            if ($fktStartPos === 0 || $fktEndPos === 0) {
                $ls_error = "CONVERT FAILED<br>";
                $ls_error .= "Function $functionName<br>";
                $ls_error .= "Part {$fktStartPos} to {$fktEndPos}<br>";
                $ls_error .= "SQL: " . $this->stmtOriginal;
                ErrorHandler::handleError("SQLConverterSybaseMySQL", $ls_error);
            }

            // $sqlOld = "CONVERT(datetime, '2021-04-28')"
            $sqlOld = substr($stmtConverted, $fktStart, $cnt - $fktStart);
            // $convertParams = "datetime, '2021-04-28'"
            $convertParams = substr($stmtConverted, $fktStartPos, $fktEndPos - $fktStartPos - 1);

            // Funktionsspezifische Konvertierung durchführen
            // $sqlNew = "STR_TO_DATE('2021-04-28','%Y.%m.%d')"
            switch (strtolower($functionName)) {
                case "convert":
                    $sqlNew = $this->convertConvert($convertParams);
                    break;
                case "datepart":
                    $sqlNew = $this->convertDatepart($convertParams);
                    break;
                case "datediff":
                    $sqlNew = $this->convertDatediff($convertParams);
                    break;
                case "dateadd":
                    $sqlNew = $this->convertDateadd($convertParams);
                    break;
                default:
                    ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function not found ({$functionName})");
            }

            // Konvertierten Teil im Original-SQL einsetzen
            $stmtConverted = str_replace($sqlOld, $sqlNew, $stmtConverted);
        }
    }

    /**
     * Splittet einen String anhand von Komma auf Beginn + Inhalt + Ende auf
     * Maximal zurückgegebene Array-Größe = 3
     * von: p1,p2,p3,...,p99
     * auf: p1,p2-p98,p99
     * @param string $param - z.B.: datetime,date_format(ast.createdate,'%d.%m.%Y'),104
     * @return array - z.B.: [datetime, date_format(ast.createdate,'%d.%m.%Y'), 104]
     */
    private function getParams(string $param): array
    {
        $ret = array();

        $cntParam = 0;
        $cntLevel = 0;
        foreach (str_split($param) as $chrPart) {
            $cnt++;
            if ($chrPart === ',' && $cntLevel === 0) {
                // Level 0 - neuer Parameter wird gestartet
                $cntParam++;
                // Beistrich selbst nicht als Parameter-Zeichen aufnehmen
                continue;
            }
            if ($chrPart === '(') {
                $cntLevel++;
            }
            if ($chrPart === ')') {
                $cntLevel--;
            }
            $ret[$cntParam] .= $chrPart;
        }

        return $ret;
    }

    /**
     * Konvertiert die Sybase Funktion convert(...)
     * @param $params - z.B.: "varchar(10),now(),104"
     * @return string
     */
    private function convertConvert(string $params): string
    {
        $e = $this->getParams($params);
        if (sizeof($e) !== 2 && sizeof($e) !== 3) {
            // Falsche Parameteranzahl - keine Konvertierung möglich
            ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function convert - Wrong number of parameters<br>Details: " . var_export($params, true));
        }

        $paramsConverted = "";
        $convertFunction = "";

        $typ = strtolower(trim($e[0])); // varchar(10)
        $typLength = null; // 10
        $val = $e[1]; // now()
        $substring = false;

        // Typ-Konvertierung
        if ($typ === "datetime") {
            $convertFunction = "Date";
            $paramsConverted .= "STR_TO_DATE";
        }
        else if (strpos($typ, "char") !== false) {
            $convertFunction = "Date";

            // Handling char(7), varchar(10), ...
            preg_match('/char\((\d*)\)/i', $typ, $out);
            if (sizeof($out) === 2) {
                $typLength = (int)$out[1];
                $paramsConverted .= "SUBSTRING(";
                $substring = true;
            }

            $paramsConverted .= "DATE_FORMAT";
        }
        else if (strpos($typ, "int") !== false) {
            $convertFunction = "Int";
            $paramsConverted .= "CAST";
        }
        else if (strpos($typ, "decimal") !== false) {
            $convertFunction = "Decimal";
            $paramsConverted .= "CAST";
        }
        else {
            ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function convert - Wrong type ({$typ})");
        }

        // Format-Konvertierung
        switch ($convertFunction) {
            case "Date":
                // Value-Konvertierung
                if (sizeof($e) < 3) {
                    // z.B: convert(varchar,KON.END_ID)
                    // Keine Konvertierung durchführen
                    $paramsConverted = trim($val);
                    break;
                }

                $paramsConverted .= "(" . trim($val);
                $paramsConverted .= "," . $this->convertDateFormat((int)$e[2]);
                $paramsConverted .= ")";
                if ($substring) {
                    $paramsConverted .= ",1," . $typLength . ")";
                }
                break;
            case "Int":
                // Value-Konvertierung
                $paramsConverted .= "(" . trim($val);
                $paramsConverted .= " AS SIGNED";
                $paramsConverted .= ")";
                break;
            case "Decimal":
                // Value-Konvertierung
                $paramsConverted .= "(" . trim($val);
                $paramsConverted .= " AS " . $typ;
                $paramsConverted .= ")";
                break;
            default:
                ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function convert - No specific convert-function found ({$typ})");
        }

        return $paramsConverted;
    }

    /**
     * Konvertiert die Sybase Funktion datepart(...)
     * @param string $params
     * @return string
     */
    private function convertDatepart(string $params): string
    {
        $e = $this->getParams($params);
        if (sizeof($e) !== 2) {
            // Falsche Parameteranzahl - keine Konvertierung möglich
            ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function datepart - Wrong number of parameters<br>Details: " . var_export($params, true));
        }

        $ret = "";

        $typ = strtolower(trim($e[0])); // quarter
        $val = trim($e[1]); // now()

        switch ($typ) {
            case "yy":
            case "year":
                $ret .= "YEAR({$val})";
                break;
            case "qq":
            case "quarter":
                $ret .= "QUARTER({$val})";
                break;
            case "mm":
            case "month":
                $ret .= "MONTH({$val})";
                break;
            case "wk":
            case "week":
                $ret .= "WEEK({$val})+1";
                break;
            case "dd":
            case "day":
                $ret .= "DAY({$val})";
                break;
            case "dy":
            case "dayofyear":
                $ret .= "DAYOFYEAR({$val})";
                break;
            case "dw":
            case "weekday":
                $ret .= "DAYOFWEEK({$val})";
                break;
            case "hh":
            case "hour":
                $ret .= "HOUR({$val})";
                break;
            case "mi":
            case "minute":
                $ret .= "MINUTE({$val})";
                break;
            case "ss":
            case "second":
                $ret .= "SECOND({$val})";
                break;
            case "ms":
            case "millisecond":
            case "us":
            case "microsecond":
                $ret .= "MICROSECOND({$val})";
                break;
            case "cwk":
            case "calweekofyear":
                $ret .= "WEEKOFYEAR({$val})";
                break;
            case "cyr":
            case "calyearofweek":
                ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Datepart cyr/calyearofweek not supported");
                break;
            case "cdw":
            case "caldayofweek":
                $ret .= "WEEKDAY({$val})+1";
                break;
            default:
                ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function datepart - Wrong dateformat ({$format})");
        }

        return $ret;
    }

    /**
     * Konvertiert die Sybase Funktion datediff(...)
     * @param string $params
     * @param bool $add - Soll die Funktion Datediff oder Dateadd konvertieren
     * @return string
     */
    private function convertDatediff(string $params, bool $add = false): string
    {
        $e = $this->getParams($params);
        if (sizeof($e) !== 3) {
            // Falsche Parameteranzahl - keine Konvertierung möglich
            ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function " . ($add ? "dateadd" : "datediff") . " - Wrong number of parameters<br>Details: " . var_export($params, true));
        }

        $ret = "";

        $function = $add ? "TIMESTAMPADD" : "TIMESTAMPDIFF";

        $typ = strtolower(trim($e[0])); // quarter
        $val1 = trim($e[1]); // now()
        $val2 = trim($e[2]); // '2021.11.03'

        switch ($typ) {
            case "yy":
            case "year":
                $ret .= $function . "(YEAR,{$val1},{$val2})";
                break;
            case "qq":
            case "quarter":
                $ret .= $function . "(QUARTER,{$val1},{$val2})";
                break;
            case "mm":
            case "month":
                $ret .= $function . "(MONTH,{$val1},{$val2})";
                break;
            case "wk":
            case "week":
                $ret .= $function . "(WEEK,{$val1},{$val2})";
                break;
            case "dd":
            case "day":
                $ret .= $function . "(DAY,{$val1},{$val2})";
                break;
            case "dy":
            case "dayofyear":
                ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Datediff part dy/dayofyear not supported");
                break;
            case "dw":
            case "weekday":
                ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Datediff part dw/weekday not supported");
                break;
            case "hh":
            case "hour":
                $ret .= $function . "(HOUR,{$val1},{$val2})";
                break;
            case "mi":
            case "minute":
                $ret .= $function . "(MINUTE,{$val1},{$val2})";
                break;
            case "ss":
            case "second":
                $ret .= $function . "(SECOND,{$val1},{$val2})";
                break;
            case "ms":
            case "millisecond":
            case "us":
            case "microsecond":
                $ret .= $function . "(MICROSECOND,{$val1},{$val2})";
                break;
        }

        return $ret;
    }

    /**
     * Konvertiert die Sybase Funktion dateadd(...)
     * @param string $params
     * @param bool $add - Soll die Funktion Dateadd konvertieren
     * @return string
     */
    private function convertDateadd(string $params): string
    {
        return $this->convertDatediff($params, true);
    }

    /**
     * Konvertiert das übergebene Datumformat von Sybase auf MySQL
     * @param int $format
     * @return string
     */
    private function convertDateFormat(int $format): string
    {
        $ret = "";

        switch ($format) {
            case 101:
                $ret = "%m/%d/%Y";
                break;
            case 2:
                $ret = "%y.%m.%d";
                break;
            case 102:
                $ret = "%Y.%m.%d";
                break;
            case 3:
                $ret = "%d/%m/%y";
                break;
            case 103:
                $ret = "%d/%m/%Y";
                break;
            case 4:
                $ret = "%d.%m.%y";
                break;
            case 104:
                $ret = "%d.%m.%Y";
                break;
            case 5:
                $ret = "%d-%m-%y";
                break;
            case 105:
                $ret = "%d-%m-%Y";
                break;
            case 6:
                $ret = "%d %b %y";
                break;
            case 106:
                $ret = "%d %b %Y";
                break;
            case 7:
                $ret = "%b %d, %y";
                break;
            case 107:
                $ret = "%b %d, %Y";
                break;
            case 8:
            case 108:
                $ret = "%H:%i:%s";
                break;
            case 9:
            case 109:
                $ret = "%b %d %Y %h:%i:%s:%f%p";
                break;
            case 10:
                $ret = "%m-%d-%y";
                break;
            case 110:
                $ret = "%m-%d-%Y";
                break;
            case 11:
                $ret = "%y/%m/%d";
                break;
            case 111:
                $ret = "%Y/%m/%d";
                break;
            case 12:
                $ret = "%y%m%d";
                break;
            case 112:
                $ret = "%Y%m%d";
                break;
            case 13:
                $ret = "%y/%d/%m";
                break;
            case 113:
                $ret = "%Y/%d/%m";
                break;
            case 14:
                $ret = "%m/%y/%d";
                break;
            case 114:
                $ret = "%m/%Y/%d";
                break;
            case 15:
                $ret = "%d/%y/%m";
                break;
            case 115:
                $ret = "%d/%Y/%m";
                break;
            case 16:
            case 116:
                $ret = "%b %d %Y %H:%i:%s";
                break;
            case 17:
            case 117:
                $ret = "%h:%i%p";
                break;
            case 18:
            case 118:
                $ret = "%H:%i";
                break;
            case 19:
                $ret = "%h:%i:%s:%f%p";
                break;
            case 20:
                $ret = "%H:%i:%s:%f";
                break;
            case 21:
                $ret = "%y/%m/%d %H:%i:%s";
                break;
            case 22:
                $ret = "%y/%m/%d %h:%i%p";
                break;
            case 23:
                $ret = "%Y-%m-%dT%H:%i:%s";
                break;
            case 36:
            case 136:
                $ret = "%h:%i:%s.%f%p";
                break;
            case 37:
            case 137:
                $ret = "%H:%i:%s.%f";
                break;
            case 38:
                $ret = "%b %d %y %h:%i:%s.%f%p";
                break;
            case 138:
                $ret = "%b %d %Y %h:%i:%s.%f%p";
                break;
            case 39:
                $ret = "%b %d %y %H:%i:%s.%f";
                break;
            case 139:
                $ret = "%b %d %Y %H:%i:%s.%f";
                break;
            case 40:
                $ret = "%y-%m-%d %H:%i:%s.%f";
                break;
            case 140:
                $ret = "%Y-%m-%d %H:%i:%s.%f";
                break;
            default:
                ErrorHandler::handleError("SQLConverterSybaseMySQL", "CONVERT FAILED<br>Function convert - Wrong dateformat ({$format})");
        }

        return "'" . $ret . "'";
    }
}