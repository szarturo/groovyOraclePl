//SimDiasAntiguedad
import groovy.sql.Sql

//CONEXION A ORACLE
sql = Sql.newInstance("jdbc:oracle:thin:@localhost:1521:XE", "sim181110","sim" , "oracle.jdbc.driver.OracleDriver")

//PARAMETROS DE ENTRADA
def cveGpoEmpresa = 'SIM'
def cveEmpresa = 'CREDICONFIA'
Date fechaCierre = new Date('2010/09/14') //AAAA/MM/DD

//OBTIENE EL VALOR DE LA DEUDA MINIMA
rowDeudaMinima = sql.firstRow("""
        SELECT IMP_DEUDA_MINIMA
         FROM SIM_PARAMETRO_GLOBAL 
         WHERE CVE_GPO_EMPRESA  = ${cveGpoEmpresa}
         AND   CVE_EMPRESA      = ${cveEmpresa}
""")
def impDeudaMinima = rowDeudaMinima.IMP_DEUDA_MINIMA

//OBTIENE ARREGLO DE LOS PRESTAMOS
def curPrestamos = []
sql.eachRow("""
    SELECT P.ID_PRESTAMO, P.ID_GRUPO
    FROM SIM_PRESTAMO P
    WHERE P.CVE_GPO_EMPRESA = ${cveGpoEmpresa}
    AND P.CVE_EMPRESA = ${cveEmpresa}
    ORDER BY P.ID_PRESTAMO
""") {
  curPrestamos << it.toRowResult()
}    

//ITERA LOS PRESTAMOS
curPrestamos.each{ vlBufPrestamo ->

    Date fechaDiasAntiguedad
    def diasAntiguedad = 0
    def valorEncontrado = false

    //OBTIENE LA FORMA DE DISTRIBUCION DE PAGOS DEL PRESTAMO
    rowDistribucionPago = sql.firstRow("""
        SELECT NVL(P.ID_FORMA_DISTRIBUCION,0) ID_FORMA_DISTRIBUCION
        FROM SIM_PRESTAMO P, SIM_CAT_FORMA_DISTRIBUCION D
        WHERE P.CVE_GPO_EMPRESA = ${cveGpoEmpresa}
        AND P.CVE_EMPRESA = ${cveEmpresa}
        AND P.ID_PRESTAMO = ${vlBufPrestamo.ID_PRESTAMO}
        AND D.CVE_GPO_EMPRESA(+) = P.CVE_GPO_EMPRESA
        AND D.CVE_EMPRESA(+) = P.CVE_EMPRESA
        AND D.ID_FORMA_DISTRIBUCION(+) = P.ID_FORMA_DISTRIBUCION
    """)
    def idDistribucionPago = rowDistribucionPago.ID_FORMA_DISTRIBUCION
    
    if (idDistribucionPago == 1){
        //EL CALCULO DE DIAS DE ANTIGUEDAD SE HACE SOBRE EL CAPITAL
        
        //OBTIENE LA SUMA TOTAL DE PAGOS APLICADOS A CAPITAL DEL PRESTAMO
        rowSumaPago = sql.firstRow("""    
            SELECT SUM(TA.IMP_CAPITAL_AMORT_PAGADO) IMP_CAPITAL_AMORT_PAGADO
            FROM SIM_TABLA_AMORTIZACION TA
            WHERE TA.CVE_GPO_EMPRESA = ${cveGpoEmpresa}
            AND TA.CVE_EMPRESA = ${cveEmpresa}
            AND TA.ID_PRESTAMO = ${vlBufPrestamo.ID_PRESTAMO}
        """)
        def sumaPagosCapital = rowSumaPago.IMP_CAPITAL_AMORT_PAGADO
        
        //SE DEFINE EL ARREGLO DE CADA IMPORTE QUE SE TIENE QUE PAGAR A CAPITAL Y SU FECHA DE AMORTIZACION
        def curAmortizacionesCapital = []
        sql.eachRow("""
          SELECT IMP_CAPITAL_AMORT, FECHA_AMORTIZACION
           FROM SIM_TABLA_AMORTIZACION
           WHERE CVE_GPO_EMPRESA   = ${cveGpoEmpresa}
           AND CVE_EMPRESA         = ${cveEmpresa}
           AND ID_PRESTAMO         = ${vlBufPrestamo.ID_PRESTAMO}
           ORDER BY NUM_PAGO_AMORTIZACION   """) {
          curAmortizacionesCapital << it.toRowResult()
        }    
        
        //ITERA EL ARREGLO DE CADA IMPORTE QUE SE TIENE QUE PAGAR A CAPITAL Y SU FECHA DE AMORTIZACION
        curAmortizacionesCapital.each{ vlBufAmortizaciones ->
            sumaPagosCapital = sumaPagosCapital - vlBufAmortizaciones.IMP_CAPITAL_AMORT
            if  (!valorEncontrado ){
                //VERIFICA SI ALCANZA A PAGAR EL CAPITAL EN LA FECHA DE AMORTIZACION CORRESPONDIENTE
                if (sumaPagosCapital < -impDeudaMinima && fechaCierre >=  vlBufAmortizaciones.FECHA_AMORTIZACION){ 
                        fechaDiasAntiguedad =  vlBufAmortizaciones.FECHA_AMORTIZACION
                        diasAntiguedad = fechaCierre - fechaDiasAntiguedad
                        valorEncontrado = true
                }
            }
        }   
        
        println "Prestamo: ${vlBufPrestamo.ID_PRESTAMO}"     
        println "Dias Antiguedad: ${diasAntiguedad}"     
        
        //ACTUALIZA LOS DIAS DIAS DE ANTIGUEDAD EN LOS PRESTAMOS
        sql.executeUpdate """
            UPDATE SIM_PRESTAMO
               SET NUM_DIAS_ANTIGUEDAD = ${diasAntiguedad}
             WHERE CVE_GPO_EMPRESA     = ${cveGpoEmpresa}
               AND CVE_EMPRESA         = ${cveEmpresa}
               AND ID_PRESTAMO         = ${vlBufPrestamo.ID_PRESTAMO}
        """        

        //ACTUALIZA LOS DIAS DIAS DE ANTIGUEDAD EN LOS PRESTAMOS GRUPALES
        sql.executeUpdate """
            UPDATE SIM_PRESTAMO_GRUPO
               SET NUM_DIAS_ANTIGUEDAD = CASE WHEN ${diasAntiguedad} >= NVL(NUM_DIAS_ANTIGUEDAD,0) THEN ${diasAntiguedad}
                                              ELSE NUM_DIAS_ANTIGUEDAD
                                         END
             WHERE CVE_GPO_EMPRESA     = ${cveGpoEmpresa}
               AND CVE_EMPRESA         = ${cveEmpresa}
               AND ID_GRUPO            = ${vlBufPrestamo.ID_GRUPO}
        """        
    
    }else{
        //EL CALCULO DE DIAS DE ANTIGUEDAD SE HACE SOBRE EL ULTIMO ACCESORIO EN EL ORDEN DE PAGO
    }
}//ITERA LOS PRESTAMOS