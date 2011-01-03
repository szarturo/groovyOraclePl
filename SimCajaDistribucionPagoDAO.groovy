//SimCajaDistribucionPagoDAO
import groovy.sql.Sql

//CONEXION A ORACLE
sql = Sql.newInstance("jdbc:oracle:thin:@localhost:1521:XE", "sim181110","sim" , "oracle.jdbc.driver.OracleDriver")

//PARAMETROS DE ENTRADA
def cveGpoEmpresa = 'SIM'
def cveEmpresa = 'CREDICONFIA'
def importe = 1000
def fechaAplicacion = '15-09-2010'
def idPrestamoGrupo = 5
def sTxrespuesta
def arregloResultado

def PKG_CREDITO = new PKG_CREDITO()
arregloResultado =  PKG_CREDITO.fCalculaProporcion(cveGpoEmpresa, cveEmpresa, idPrestamoGrupo, fechaAplicacion,importe,sTxrespuesta, sql)

def sumaImporte = 0

arregloResultado.each{ 
    sumaImporte = sumaImporte + it.IMP_DEUDA
    println "Id Prestamo: ${it.ID_PRESTAMO} Nombre: ${it.NOM_COMPLETO} Pago: ${it.IMP_DEUDA}"
}

println "Suma importe: ${sumaImporte}"
