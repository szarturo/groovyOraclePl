//SimPrestamoMovimientoExtraordinario
import groovy.sql.Sql

//CONEXION A ORACLE
sql = Sql.newInstance("jdbc:oracle:thin:@localhost:1521:XE", "sim181110","sim" , "oracle.jdbc.driver.OracleDriver")

//PARAMETROS DE ENTRADA
def cveGpoEmpresa = 'SIM'
def cveEmpresa = 'CREDICONFIA'
def cveUsuario = 'administrador'
def fechaMovimiento = '15/09/2010'
def idPrestamo = 1
def cveOperacion = 'CRCNDINTE' //CONDONACIÓN DE INTERÉS
//'CRCARINT' //CARGO DE INTERESES
def cveConcepto = 'INTERE' //PREGUNTAR COMO SE OBTUVO DESDE LA JSP LA CLAVE CONCEPTO
def numAmort
def impNeto = 25.00

//Operacion: PREMOVTO

//OBTIENE EL ID PREMOVIMIENTO
rowPremovimiento = sql.firstRow("SELECT SQ01_PFIN_PRE_MOVIMIENTO.nextval as ID_PREMOVIMIENTO FROM DUAL")
def sIdPreMovimiento = rowPremovimiento.ID_PREMOVIMIENTO

//OBTIENE LA CUENTA DEL PRESTAMO
rowCuenta = sql.firstRow("""
    SELECT ID_CUENTA 
        FROM SIM_PRESTAMO
        WHERE CVE_GPO_EMPRESA = ${cveGpoEmpresa}
        AND CVE_EMPRESA = ${cveEmpresa}
        AND ID_PRESTAMO =  ${idPrestamo}
        """)
def sIdCuenta = rowCuenta.ID_CUENTA
println "Id Cuenta: ${sIdCuenta}"

//OBTIENE LA FECHA DE LIQUIDACION
rowFechaLiquidacion = sql.firstRow("""
    SELECT TO_CHAR(TO_DATE(F_MEDIO,'DD-MM-YYYY'),'DD-MON-YYYY') AS F_LIQUIDACION 
                FROM PFIN_PARAMETRO
                WHERE CVE_GPO_EMPRESA = ${cveGpoEmpresa}
                AND CVE_EMPRESA = ${cveEmpresa}
                AND CVE_MEDIO = 'SYSTEM' 
    """)
def sFLiquidacion = rowFechaLiquidacion.F_LIQUIDACION
//EL AÑO DEL CAMPO F_MEDIO ES IGUAL SIEMPRE A 0010
//FECHA_MEDIO = FECHA_SISTEMA = FECHA_LIQUIDACION
println "Fecha Liquidación: ${sFLiquidacion}"

//OBTIENE LA FECHA DE APLICACION
//LA FECHA LA OBTIENE DE LA CAPTURA DE LA PANTALLA, AQUI SOLO APLICA FORMATO
rowFechaAplicacion = sql.firstRow("SELECT TO_CHAR(TO_DATE(${fechaMovimiento},'DD-MM-YYYY'),'DD-MON-YYYY') F_APLICACION FROM DUAL ")
def sFechaAplicacion = rowFechaAplicacion.F_APLICACION
println "Fecha Aplicación: ${sFechaAplicacion}"

String sCveGpoEmpresa = cveGpoEmpresa
String sCveEmpresa = cveEmpresa
String sIdPreMovi = sIdPreMovimiento
String sFMovimiento = sFLiquidacion
//String sIdCuenta = sIdCuenta
String sIdPrestamo = idPrestamo
String sCveDivisa = "MXP"
String sCveOperacion = cveOperacion
String sImpNeto = impNeto
String sCveMedio = "PRESTAMO"
String sCveMercado = "PRESTAMO"
String sNota = "Movimiento extraordinario"
String sIdGrupo = ""
String sCveUsuario = cveUsuario
String sFValor = sFechaAplicacion
String sNumPagoAmort = "0"
String pTxrespuesta =""

//Se genera el premovimiento      
def PKG_PROCESOS = new PKG_PROCESOS()

PKG_PROCESOS.pGeneraPreMovto(sCveGpoEmpresa,sCveEmpresa,sIdPreMovi,sFMovimiento,sIdCuenta,sIdPrestamo,
                      sCveDivisa,sCveOperacion,sImpNeto,sCveMedio,sCveMercado,sNota,sIdGrupo,
                      sCveUsuario,sFValor,sNumPagoAmort,pTxrespuesta,sql)
                      
PKG_PROCESOS.pGeneraPreMovtoDet(sCveGpoEmpresa, sCveEmpresa, sIdPreMovi, 
                     cveConcepto, sImpNeto,sNota, pTxrespuesta, sql) 
                     
//Se procesa el movimiento                          
def PKG_PROCESADOR_FINANCIERO = new PKG_PROCESADOR_FINANCIERO()

//¿PARA QUE ENVIAR SIT_MOVIMIENTO = PV? SIEMPRE ES IGUAL A PV
def vlIdMovimiento = PKG_PROCESADOR_FINANCIERO.pProcesaMovimiento(sCveGpoEmpresa, sCveEmpresa, sIdPreMovi, 'PV',sCveUsuario, 'F', pTxrespuesta, sql);
println "Id Movimiento: ${vlIdMovimiento}"

def PKG_CREDITO = new PKG_CREDITO()
pTxRespuesta =  PKG_CREDITO.pActualizaTablaAmortizacion(sCveGpoEmpresa, sCveEmpresa, vlIdMovimiento, sql)

