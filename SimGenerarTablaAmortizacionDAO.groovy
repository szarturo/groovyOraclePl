import groovy.sql.Sql

//CONEXION A ORACLE
sql = Sql.newInstance("jdbc:oracle:thin:@localhost:1521:XE", "sim181110","sim" , "oracle.jdbc.driver.OracleDriver")

//PARAMETROS DE ENTRADA
def cveGpoEmpresa = 'SIM'
def cveEmpresa = 'CREDICONFIA'
def idPrestamo = 1
def sTxrespuesta
//def cveUsuario = 'administrador'
//def fechaMovimiento = '15/09/2010'


def PKG_CREDITO = new PKG_CREDITO()
sTxrespuesta =  PKG_CREDITO.pGeneraTablaAmortizacion(cveGpoEmpresa, cveEmpresa, idPrestamo, sTxrespuesta, sql)
