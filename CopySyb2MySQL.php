<?php
// This script is provided as is with no warranties.
// Copyright ARA AG https://www.ara.at 
// APACHE LICENSE, VERSION 2.0, https://www.apache.org/licenses/LICENSE-2.0
// Gets data out of a Sybase SQL Server database and copies it to a MySQL database.
// IT/WIE, 2020-12-20
// IT/WIE, 2021-01-08 Insert statements can alternatively be written in a textfile.

// Umgebungsvariable prüfen
if ($argc <> 3) die2("Aufruf: GetSybData.php sourcedb targetdb");

// Variable
$mysql_server = 'some.mysql.server';
$mysql_user   = 'some user name';
$mysql_pass   = 'some pass';
$db           = $argv[1];
$db_dest      = $argv[2];
$tab_arr      = array();            // array containing all tables of the database
$filelnmax    = 1000;               // max rowcount for the output to one file
$lnactual     = 0;                  // actual rowcount in a file
$fileno       = 0;                  // number of files generated
$filepath     = 'path to output files';

// Dateiname der Output-Files
$filename = 'MySQL_'.$db_dest;

// Logfile anlegen
$log = new Log2();
$log->WriteLog('Sybase-Daten nach MySQL schreiben.');
$log->WriteLog("Quell-Datenbank: $db");
$log->WriteLog("Ziel-Datenbank:  $db_dest");

// Mit Sybase-DB verbinden
$odbc = odbc_connect('name of DSN','username','password');
if ($odbc === false) die2("Fehler beim Verbinden mit der Sybase DB!");

// Mit MySQL Server verbinden
$my = mysqli_connect($mysql_server,$mysql_user,$mysql_pass,$db_dest);
if ($my === false) die2("Fehler beim Verbinden mit dem MySQL Server");

// Charset am MySQL-Server setzen
mysqli_set_charset($my,'latin1');

// File anlegen in das MYSQLSH.exe für jedes File aufgerufen wird
$mysqlsh = 'mysqlsh.exe -h SERVERNAME -u USERNAME --password="PASSWORD" -D "'.$db_dest.'" -f PATH';
$f2 = fopen($filepath.'\\'.$filename.'.txt','w');

// Tabellen vom Sybase-Server einlesen
$query = "use $db";
OdbcQuery($odbc,$query);
$query = "select id 'id', name 'name' from sysobjects where type = 'U' and id in (0)";
//$query = "select id 'id', name 'name' from sysobjects where type = 'U' order by id";
$result = OdbcQuery($odbc,$query);
while ($row = odbc_fetch_array($result)) {

    // Variable
    $tab_id    = $row['id'];
    $tab_name  = $row['name'];
    $tab_arr[] = $row['name'];
    $select    = '';
    $insert    = '';
    $insert2   = '';
    $n         = 1;
    $i         = 0;
    $feld_arr  = array();

    // Log
    $log->WriteLog("Tabelle: $tab_name");
    $log->WriteLog("Tab-ID:  $tab_id");

    // Felder der Tabelle holen
    $query = "select c.name 'spalte', t.name 'typ', c.length 'length'
        from syscolumns c
            inner join systypes t on t.usertype = c.usertype
        where c.id = $tab_id";
    $res2 = OdbcQuery($odbc,$query);
    while ($row2 = odbc_fetch_array($res2)) {

        // Variable
        $col_name   = $row2['spalte'];
        $col_type   = $row2['typ'];
        $col_length = $row2['length'];

        // Array mit Datentyp
        $arrindex = "A$n";
        $feld_arr[$arrindex] = array('name' => $col_name, 'type' => $col_type);

        // Select zusammenbauen
        if ($select == '') $select = 'select '.$col_name." 'A$n', case when $col_name is null then 1 else 0 end 'A$n"."a'";
        else $select .= ', '.$col_name." 'A$n', case when $col_name is null then 1 else 0 end 'A$n"."a'";

        // Zähler
        $n++;
    }       // Ende Felder einlesen

    // Select fertigstellen
    $select .= " from $tab_name";
    //echo $select.$ln;
    //var_dump($feld_arr);

    // Insert zusammenbauen - Felder
    reset($feld_arr);
    foreach ($feld_arr as $key => $value) {
        if ($insert == '') $insert = "insert into $tab_name ({$value['name']}";
        else $insert .= ', '.$value['name'];
    }
    $insert .=') values (';
    //echo $insert.$ln.$ln;

    // Daten vom Sybase-Server holen
    $res2 = OdbcQuery($odbc,$select);
    while ($row2 = odbc_fetch_array($res2)) {

        // Variable
        $insert2 = '';

        // Array mit den Daten durchgehen
        reset($feld_arr);
        foreach ($feld_arr as $k => $v) {

            // Variable
            $wert = '';                 // Wert der schlussendlich in das Insert eingefügt wird.

            // Festlegen, ob der Wert NULL ist
            $z = $k.'a';
            if ($row2[$z] == 1) $wert = 'NULL';
            else {

                // Festlegen ob es Anführungszeichen braucht bei CHAR und DATETIME Feldern
                if ($v['type'] != 'smallint' && $v['type'] != 'int' && $v['type'] != 'decimal') {
                    $row2[$k] = mysqli_escape_string($my,$row2[$k]);
                    $wert = "'{$row2[$k]}'";
                }
                else $wert = $row2[$k];
            }

            // Inhalt von etwaigen Leerzeichen befreien
            $wert = ltrim(rtrim($wert));

            // Insert zusammenbauen
            if ($insert2 == '') $insert2 = ''.$wert;
            else $insert2 .= ', '.$wert;
        }

        // Insert fertigstellen
        $insert3 = $insert.$insert2.')';
        $lnactual++;
		
		/////////////////////////
        // Insert in file
		/////////////////////////
        /*
        if ($lnactual >= $filelnmax || $fileno == 0) {
            $lnactual = 0;
            $fileno++;
            $log->WriteLog("File Nr: $fileno");
            if (isset($fp)) fclose($fp);
            $fp = CreateFile($fileno);
            fwrite($fp,'use '.$db_dest.';'.$ln);
        }

        // Insert in Datei schreiben
        fwrite($fp,$insert3.';'.$ln);
        */
		/////////////////////////
        // Insert am MySQL Server
		/////////////////////////
        $resmy = mysqli_query($my,$insert3);
        if ($resmy === false) {
            echo "Fehlernummer: ".mysqli_errno($my).$ln;
            echo "Fehler: ".mysqli_error($my).$ln;
            echo $insert3.$ln.$ln;
            die();
        }

        // Variable zurücksetzen
        $insert2 = '';
        $insert3 = '';

        // Zähler
        $i++;

    }       // Ende Daten vom Sybase-Server holen

    // Log
    $log->WriteLog("$i Zeilen geschrieben");
}           // Ende alle Tabellen durchgehen

// Prüfen, ob in Ziel- und Quelldatenbank die gleiche Anzahl an Datensätzen enthalten sind
reset($tab_arr);
foreach ($tab_arr as $key => $value) {

    // Variable
    $anz_syb = 0;
    $anz_my  = 0;

    // Select zusammenbauen
    $query = "select count(*) 'anz' from ".$value;

    // Anzahl der Datensätze aus der Sybase-DB lesen
    $res2 = OdbcQuery($odbc,$query);
    while ($row2 = odbc_fetch_array($res2)) {
        $anz_syb = $row2['anz'];
    }

    // Anzahl der Datensätze aus der MySQL-DB lesen
    $res2 = mysqli_query($my,$query);
    while ($row2 = mysqli_fetch_array($res2,MYSQLI_NUM)) {
        $anz_my = $row2[0];
    }

    // Log
    $log->WriteLog("Anzahl Rows Sybase: $anz_syb, Anzahl Rows MySQL: $anz_my");

    // Ergebnisse gegenüberstellen
    if ($anz_syb != $anz_my) {
        $log->WriteLog("Fehler! Unterschiedliche Anzahl Rows!");
        die;
    }
}       // Ende Prüfen, ob Ziel- und Quelltabelle die gleiche Anzahl an Feldern hat

// Datenbankverbindungen schließen
mysqli_close($my);
odbc_close($odbc);

// Datei schließen
fclose($f2);

///
/// Funktionen
/// 

// Legt ein neues File an in das die Insert-Befehle geschrieben werden
// Returns Filepointer.
function CreateFile($lfd) {

    // Variable
    global $filepath, $filename, $f2, $mysqlsh, $ln;

    // Dateiname zusammenbauen
    $lfd = str_pad($lfd,4,'0',STR_PAD_LEFT);            // Zahl mit vorlaufenden Nullen
    $filename2 = $filename.'_'.$lfd.'.sql';

    // Datei anlegen
    $fp = fopen($filepath.'\\'.$filename2,'w');

    // Eintrag in MYSQLSH-File schreiben
    fwrite($f2,$mysqlsh.$filename2.$ln);

    // Funktion beenden mit Rückgabewert
    return $fp;
}

///////////////////////////////////////////////////////////////
// Klasse zur Behandlung von Ausgaben in ein Logfile
// Zusätzlich zum Logfile wird nach Stdout geschrieben
class Log2 {
	
	// Variablen anlegen
	private $fp;						// Filepointer zum Logfile
	public  $path;						// Pfad des Lofiles
	public  $file;						// Name der Logdatei
	private $ln;						// Zeilenschaltung
	private $t;							// Tabulator
	public  $begintime;					// Zeitpunkt der Erstellung des Logfiles
	
	// Constructor der Klasse: legt das Logfile an und initialisiert Variable
	function __construct() {
		
		// Variable initialisieren
		$this->ln   = chr(13).chr(10);
		$this->path = LOGPATH.'/';
		$this->file = basename($GLOBALS['argv'][0]).'.txt';
		$this->t    = chr(9);
		
		// Beginnzeit festlegen
		$this->begintime = time();
		
		// Logfile anlegen
		$this->fp = fopen($this->path.$this->file,'a+');
		if ($this->fp === false) die("Fehler beim Oeffnen der Logdatei!");
		
		// Beginnzeit ins Log schreiben
		$begin = "##################################".$this->ln.
	             "Programmstart: ".date('d.m.Y H:i:s');
		$this->WriteLog($begin);
		
	}		// End Constructor
	
	// Destructor der Klasse: schie�t das Logfile
	function __destruct() {
		
		// Ausf�hrungsdauer des Programms berechnen
		$dauer = time() - $this->begintime;
		
		// Ende-Datum in Logfile schreiben
		$this->WriteLog("Programmende: ".date('d.m.Y H:i:s'));
		$this->WriteLog("Ausfuehrungszeit: $dauer Sekunden".$this->ln);
		
		// Logfile schlie�en
		fclose($this->fp);
		
	}		// End Destructor
	
	// Text in das Logfile schreiben
	// $text = Text der in das Logfile geschrieben werden soll
	//         Zeilenschaltung am Ende f�gt die Funktion automatisch ein
	public function WriteLog($text) {
		$rueck = fwrite($this->fp,$text.$this->ln);
		if ($rueck === false) die("Fehler beim Schreiben ins Logfile!");
		echo $text.$this->ln;							// Ausgabe nach Stdout
	}
	
	// Text in das Logfile schreiben
	// $text = Text der in das Logfile geschrieben werden soll
	//         Tabulator am Ende f�gt die Funktion automatisch ein
	public function WriteLogTab($text) {
		$rueck = fwrite($this->fp,$text.$this->t);
		if ($rueck === false) die("Fehler beim Schreiben ins Logfile!");
		echo $text.$this->t;							// Ausgabe nach Stdout
	}
}		// End Class Log2

// Put some text to stdout and quit script with errorlevel 1
function die2($errormsg) {
	global $ln;					// Zeilenumbuch
	echo 'Fehler:'.$ln;			// Wort "Fehler" ausgeben f�r Fehleranalyse im Log
	echo $errormsg.$ln;			// Text nach Standard-Out ausgeben
	exit(1);					// Beenden mit Errorlevel 1
}

///////////////////////////////////////////////////////////////
// Query via ODBC absetzen
// Argumente: $db = Link zur Datenbankverbindung
//            $query = Query
// Rückgabewert: Handle zum Resultset
function OdbcQuery($db,$query) {
	
	// Variable initialisieren
	global $ln;
	
	// Variable �berpr�fen
	if (!is_resource($db)) die2("Fehler! Es wurde kein gueltiger Datenbankhandle zurueckgegeben!".$ln);
	if (strlen($query) < 4) die2("Fehler! Es wurde kein gueltiges Query uebergeben!".$ln);
	
	// Query via ODBC abeschicken
	$result = odbc_exec($db,$query);
	
	// Programmabbruch wenn Fehler zur�ckgegeben wird
	if ($result === false) die2("Fehler beim Execute eines ODBC Query!".$ln.$ln.$query.$ln.$ln);
	
	// Funktion beenden mit R�ckgabewert
	return $result;
}