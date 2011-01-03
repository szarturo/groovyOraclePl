//SimGenerarTablaAmortizacionDAO
import groovy.sql.Sql

//CONEXION A ORACLE
sql = Sql.newInstance("jdbc:oracle:thin:@localhost:1521:XE", "sim181110","sim" , "oracle.jdbc.driver.OracleDriver")

//PARAMETROS DE ENTRADA
def cveGpoEmpresa = 'SIM'
def cveEmpresa = 'CREDICONFIA'
def idPrestamo = 5
def sTxrespuesta

def PKG_CREDITO = new PKG_CREDITO()
sTxrespuesta =  PKG_CREDITO.pGeneraTablaAmortizacion(cveGpoEmpresa, cveEmpresa, idPrestamo, sTxrespuesta, sql)