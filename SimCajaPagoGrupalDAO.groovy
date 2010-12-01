import groovy.sql.Sql

//CONEXION A ORACLE
sql = Sql.newInstance("jdbc:oracle:thin:@localhost:1521:XE", "sim181110","sim" , "oracle.jdbc.driver.OracleDriver")

//PARAMETROS DE ENTRADA
def sMontos = [27.56, 17.32, 22.44, 32.68]
def sIdPrestamoIndividual = [296, 299, 297, 298]
//def sIdCliente = [33682, 33685, 33683, 33684] //¿PARA QUE SE UTILIZA?

def cveGpoEmpresa = 'SIM'
def cveEmpresa = 'CREDICONFIA'
def fechaMovimiento = '15/09/2010'
def cveUsuario = 'administrador'

def rango = sIdPrestamoIndividual.size - 1

(0..rango).each { i ->
    println "*************************************"
    println "Prestamo ${i+1}"
    println "Id Prestamo Individual: ${sIdPrestamoIndividual[i]}"
    println "Monto: ${sMontos[i]}"
    
    //OBTIENE EL ID PREMOVIMIENTO
    row = sql.firstRow("SELECT SQ01_PFIN_PRE_MOVIMIENTO.nextval as ID_PREMOVIMIENTO FROM DUAL")
    def sIdPreMovimiento = row.ID_PREMOVIMIENTO
    
    //OBTIENE LA FECHA DE LIQUIDACION
    row = sql.firstRow("""SELECT TO_CHAR(TO_DATE(F_MEDIO,'DD-MM-YYYY'),'DD-MON-YYYY') AS F_LIQUIDACION 
                    FROM PFIN_PARAMETRO
                    WHERE CVE_GPO_EMPRESA = ${cveGpoEmpresa}
                    AND CVE_EMPRESA = ${cveEmpresa}
                    AND CVE_MEDIO = 'SYSTEM' """)
    def sFLiquidacion = row.F_LIQUIDACION
    //EL AÑO DEL CAMPO F_MEDIO ES IGUAL SIEMPRE A 0010
    //FECHA_MEDIO = FECHA_SISTEMA = FECHA_LIQUIDACION
    //println "Fecha Liquidación: ${sFLiquidacion}"
    
    //OBTIENE LA FECHA DE APLICACION
    //LA FECHA LA OBTIENE DE LA CAPTURA DE LA PANTALLA, AQUI SOLO APLICA FORMATO
    row = sql.firstRow("SELECT TO_CHAR(TO_DATE(${fechaMovimiento},'DD-MM-YYYY'),'DD-MON-YYYY') F_APLICACION FROM DUAL ")
    def sFechaAplicacion = row.F_APLICACION
    //println "Fecha Aplicación: ${sFechaAplicacion}"
    
    //OBTIENE LA CUENTA DEL CLIENTE
    row = sql.firstRow("""SELECT ID_CUENTA 
        FROM PFIN_CUENTA
        WHERE CVE_GPO_EMPRESA = ${cveGpoEmpresa}
        AND CVE_EMPRESA = ${cveEmpresa}
        AND CVE_TIP_CUENTA = 'VISTA'
        AND SIT_CUENTA = 'AC'
        AND ID_TITULAR = (SELECT ID_CLIENTE FROM SIM_PRESTAMO WHERE ID_PRESTAMO =  ${sIdPrestamoIndividual[i]})""")
    def sIdCuentaVista = row.ID_CUENTA
    println "Id Cuenta Vista, obtenida PFIN_CUENTA: ${sIdCuentaVista}"
    //¿CUAL ES LA DIFERENCIA ENTRE UNA CVE_TIP_CUENTA = VISTA Y CVE_TIP_CUENTA = CREDITO?
    
    String sCveGpoEmpresa = cveGpoEmpresa
    String sCveEmpresa = cveEmpresa
    String sIdPreMovi = sIdPreMovimiento
    String sFMovimiento = sFLiquidacion
    String sIdCuenta = sIdCuentaVista
    String sIdPrestamo = sIdPrestamoIndividual[i]
    String sCveDivisa = "MXP"
    String sCveOperacion = "TEDEPEFE" //DEPÓSITO DE EFECTIVO
    String sImpNeto = sMontos[i]
    String sCveMedio = "PRESTAMO"
    String sCveMercado = "PRESTAMO"
    String sNota = "Deposito de efectivo"
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

    //Se procesa el movimiento                          
    def PKG_PROCESADOR_FINANCIERO = new PKG_PROCESADOR_FINANCIERO()
    
    //¿PARA QUE ENVIAR SIT_MOVIMIENTO = PV? SIEMPRE ES IGUAL A PV
    vlIdMovimiento = PKG_PROCESADOR_FINANCIERO.pProcesaMovimiento(sCveGpoEmpresa, sCveEmpresa, sIdPreMovi, 'PV',sCveUsuario, 'F', pTxrespuesta, sql);
    println "Id Movimiento: ${vlIdMovimiento}"       
    
    //Se procesa el Credito                       
    def PKG_CREDITO = new PKG_CREDITO()
    
    PKG_CREDITO.pAplicaPagoCredito(sCveGpoEmpresa,sCveEmpresa,sIdPrestamo,sCveUsuario,sFValor,pTxrespuesta,sql)
    
}