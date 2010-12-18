//PKG_CREDITO
import java.text.SimpleDateFormat;

class PKG_CREDITO {

    //PARAMETROS DE ENTRADA
    def cSitCancelada = 'CA'
    def cFalso = 'F'
    def cVerdadero = 'V'
    def cDivisaPeso = 'MXP'
    def cPagoPrestamo = 'CRPAGOPRES' //PAGO DE PRESTAMO
    def vgFechaSistema

    String toString(){"PKG_CREDITO"}

    def AsignaFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql){
	    def row = sql.firstRow("""
		       SELECT  TO_CHAR(TO_DATE(F_MEDIO,'DD-MM-YYYY'),'DD-MON-YYYY')  F_MEDIO
			FROM    PFIN_PARAMETRO
			WHERE   CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
			    AND CVE_EMPRESA     = ${pCveEmpresa}
			    AND CVE_MEDIO       = 'SYSTEM' """)
	    return row.F_MEDIO
    }
   
    def pAplicaPagoCredito(
		pCveGpoEmpresa,
                pCveEmpresa,
                pIdPrestamo,
                pCveUsuario,
                pFValor,
                pTxrespuesta,
		sql){

        def vlImpSaldo             
        def vlIdCuenta             
        def vlImpNeto              
        def vlMovtosPosteriores    
        def vlResultado            

	println "INICIA pAplicaPagoCredito"
	def curAmortizacionesPendientes = []
	sql.eachRow("""
	  SELECT NUM_PAGO_AMORTIZACION
	    FROM SIM_TABLA_AMORTIZACION
	   WHERE CVE_GPO_EMPRESA         = ${pCveGpoEmpresa} AND
		 CVE_EMPRESA             = ${pCveEmpresa}    AND
		 ID_PRESTAMO             = ${pIdPrestamo}    AND
		 NVL(B_PAGADO,${cFalso}) = ${cFalso}
	   ORDER BY NUM_PAGO_AMORTIZACION    """) {
	  curAmortizacionesPendientes << it.toRowResult()
	}

	vgFechaSistema = AsignaFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql)

	//Se obtiene el id de la cuenta asociada al credito
	def rowCuentaCredito = sql.firstRow("""
	    SELECT ID_CUENTA_REFERENCIA
	      FROM SIM_PRESTAMO
	     WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
	       AND CVE_EMPRESA     = ${pCveEmpresa}
	       AND ID_PRESTAMO     = ${pIdPrestamo}""")
	//EN LA TABLA PFIN_CUENTA EXISTE EL CAMPO CVE_TIP_CUENTA CON LOS VALORES DE VISTA Y CREDITO
	//ID_CUENTA_REFERENCIA = VISTA
	//ID_CUENTA = CREDITO
	       
	vlIdCuenta = rowCuentaCredito.ID_CUENTA_REFERENCIA
	println "Id Cuenta obtenida de SIM_PRESTAMO: ${vlIdCuenta}"
     
	//Valida que la fecha valor no sea menor a un pago previo
	def rowMovtosPosteriores = sql.firstRow("""
	     SELECT COUNT(1) RESULTADO
		FROM PFIN_MOVIMIENTO
	       WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
		 AND CVE_EMPRESA     = ${pCveEmpresa}
		 AND ID_PRESTAMO     = ${pIdPrestamo}
		 AND SIT_MOVIMIENTO <> ${cSitCancelada}
		 AND NVL(F_APLICACION,F_LIQUIDACION) > TO_DATE(${pFValor},'DD-MM-YYYY')""")
	    
	vlMovtosPosteriores = rowMovtosPosteriores.RESULTADO
	//println "Movimientos posteriores: ${vlMovtosPosteriores}"  

	//FALTA VALIDAR LOS MOVIMIENTOS POSTERIORES

        // Ejecuta el pago de cada amortizacion mientras exista una amortizacion pendiente de pagar y el cliente 
        // tenga saldo en su cuenta	
	curAmortizacionesPendientes.each{ vlBufAmortizaciones ->   
		println "Pago Amortizacion ${vlBufAmortizaciones.NUM_PAGO_AMORTIZACION}"

		//Se obtiene el importe de saldo del cliente
		def rowSaldoCliente = sql.firstRow(""" 
		       SELECT SDO_EFECTIVO
		         FROM PFIN_SALDO
		        WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
		          AND CVE_EMPRESA     = ${pCveEmpresa}
		          AND F_FOTO          = ${vgFechaSistema}
		          AND ID_CUENTA       = ${vlIdCuenta}
		          AND CVE_DIVISA      = ${cDivisaPeso}""")
		          
		vlImpSaldo = rowSaldoCliente.SDO_EFECTIVO
		println "Saldo Cliente: ${vlImpSaldo}"
		
		if (vlImpSaldo > 0){
			println "Saldo mayor o igual a 0: ${vlImpSaldo}"
		        vlResultado = pAplicaPagoCreditoPorAmort(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo,
				 vlBufAmortizaciones.NUM_PAGO_AMORTIZACION,vlImpSaldo, vlIdCuenta, pCveUsuario, pFValor, pTxrespuesta,sql)
		}else{
			println "Saldo menor a 0: ${vlImpSaldo}"
			return "Fin PKG_CREDITO"
		}

	}

    }

    def pAplicaPagoCreditoPorAmort(
		pCveGpoEmpresa,
                pCveEmpresa,
                pIdPrestamo,
                pNumAmortizacion,
                pImportePago,
                pIdCuenta,
		pCveUsuario,
		pFValor,
		pTxrespuesta,
		sql){

	def vlImpSaldo               
	def vlIdCuentaRef            
	def vlImpNeto                
	def vlImpConcepto            = 0
	def vlImpCapitalPendLiquidar = 0
	def vlImpCapitalPrepago      = 0
	def vlIdPreMovto             
	def vlIdMovimiento           
	def vlValidaPagoPuntual      
	def vlCveMetodo              

	def curDebe = []
	sql.eachRow("""
	 SELECT DECODE(A.CVE_CONCEPTO, 'CAPITA', D.ORDEN_CAPITAL, D.ORDEN_ACCESORIO)
		* 100 + DECODE(A.CVE_CONCEPTO, 'CAPITA',0,B.ORDEN) AS ID_ORDEN, 
		A.CVE_CONCEPTO, INITCAP(C.DESC_LARGA) DESCRIPCION, ABS(ROUND(SUM(A.IMP_NETO),2)) AS IMP_NETO
	   FROM V_SIM_TABLA_AMORT_CONCEPTO A, PFIN_CAT_CONCEPTO C, SIM_PRESTAMO P, SIM_CAT_FORMA_DISTRIBUCION D, SIM_PRESTAMO_ACCESORIO B
	  WHERE A.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
	    AND A.CVE_EMPRESA           = ${pCveEmpresa}
	    AND A.ID_PRESTAMO           = ${pIdPrestamo}
	    AND A.NUM_PAGO_AMORTIZACION = ${pNumAmortizacion}
	    AND A.IMP_NETO              <> 0
	    AND A.CVE_GPO_EMPRESA       = C.CVE_GPO_EMPRESA
	    AND A.CVE_EMPRESA           = C.CVE_EMPRESA
	    AND A.CVE_CONCEPTO          = C.CVE_CONCEPTO
	    AND A.CVE_GPO_EMPRESA       = P.CVE_GPO_EMPRESA
	    AND A.CVE_EMPRESA           = P.CVE_EMPRESA
	    AND A.ID_PRESTAMO           = P.ID_PRESTAMO
	    AND P.CVE_GPO_EMPRESA       = D.CVE_GPO_EMPRESA
	    AND P.CVE_EMPRESA           = D.CVE_EMPRESA
	    AND P.ID_FORMA_DISTRIBUCION = D.ID_FORMA_DISTRIBUCION
	    AND A.CVE_GPO_EMPRESA       = B.CVE_GPO_EMPRESA(+)
	    AND A.CVE_EMPRESA           = B.CVE_EMPRESA(+)
	    AND A.ID_PRESTAMO           = B.ID_PRESTAMO(+)
	    AND A.ID_ACCESORIO          = B.ID_ACCESORIO(+)
	GROUP BY DECODE(A.CVE_CONCEPTO, 'CAPITA', D.ORDEN_CAPITAL, D.ORDEN_ACCESORIO)
		 * 100 + DECODE(A.CVE_CONCEPTO, 'CAPITA',0,B.ORDEN), A.CVE_CONCEPTO, INITCAP(C.DESC_LARGA)
	HAVING ROUND(SUM(IMP_NETO),2) < 0
	ORDER BY DECODE(A.CVE_CONCEPTO, 'CAPITA', D.ORDEN_CAPITAL, D.ORDEN_ACCESORIO)
		 * 100 + DECODE(A.CVE_CONCEPTO, 'CAPITA', 0, B.ORDEN)
	"""){
	    //println "ID_ORDEN: ${it.ID_ORDEN}, DESCRIPCION: ${it.DESCRIPCION}, IMP_NETO: ${it.IMP_NETO} "
	    curDebe << it.toRowResult()
	}

	//******************************************************************************************
	// Inicia la logica del bloque principal
	//******************************************************************************************

	// Inicializa variables
	vlImpSaldo      = pImportePago
	vlImpNeto       = 0

	//Recupera el metodo del prestamo
	def rowMetodoPrestamo = sql.firstRow(""" 
		SELECT NVL(CVE_METODO, '00') CVE_METODO
		  FROM SIM_PRESTAMO
		 WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
		       CVE_EMPRESA     = ${pCveEmpresa}    AND
		       ID_PRESTAMO     = ${pIdPrestamo}
		""")        
	vlCveMetodo = rowMetodoPrestamo.CVE_METODO
	//println "Metodo: ${vlCveMetodo}"

	//Se obtiene el id del premovimiento
	def rowIdPremovimiento = sql.firstRow("SELECT SQ01_PFIN_PRE_MOVIMIENTO.nextval as ID_PREMOVIMIENTO FROM DUAL")
	vlIdPreMovto = rowIdPremovimiento.ID_PREMOVIMIENTO
	
	//Se genera el premovimiento      
	def PKG_PROCESOS = new PKG_PROCESOS()

	PKG_PROCESOS.pGeneraPreMovto(pCveGpoEmpresa,pCveEmpresa,vlIdPreMovto,vgFechaSistema,pIdCuenta,pIdPrestamo,
		          cDivisaPeso,cPagoPrestamo,vlImpNeto,'PRESTAMO', 'PRESTAMO', 'Pago de préstamo',0,
		          pCveUsuario,pFValor,pNumAmortizacion,pTxrespuesta,sql)
		          
	curDebe.each{ vlBufDebe ->
		if (vlImpSaldo > 0){
		   if (vlBufDebe.IMP_NETO > vlImpSaldo){ 
		      //Si ya no tiene saldo para el pago del concepto no paga completo
		      vlImpConcepto  = vlImpSaldo
		   }else{
		      vlImpConcepto  = vlBufDebe.IMP_NETO;
		   }
		   
		   PKG_PROCESOS.pGeneraPreMovtoDet(pCveGpoEmpresa, pCveEmpresa, vlIdPreMovto, 
				     vlBufDebe.CVE_CONCEPTO, vlImpConcepto, vlBufDebe.DESCRIPCION, pTxrespuesta, sql)

		   // Se realiza actualiza el importe neto y el saldo del cliente
		   vlImpNeto   = vlImpNeto  + vlImpConcepto
		   vlImpSaldo  = vlImpSaldo - vlImpConcepto

		   println "${vlBufDebe.CVE_CONCEPTO} : ${vlImpConcepto}   SDO: ${vlImpSaldo} NETO: ${vlImpNeto}"                    
		}
    	}

	// Cuando es el metodo seis y aun sobra saldo, es necesario pagar capital adelantado y recalcular la tabla de 
	// amortizacion para los pagos subsecuentes
	// Solo se puede adelantar hasta el capital pendiente de pago
	if (vlImpSaldo > 0 && vlCveMetodo == '06' ){
		println "METODO = 6 AND SALDO > 0"
		//PENDIENTE POR DEFINIR
	}

	// Se actualiza el monto del premovimiento
	sql.executeUpdate """
	UPDATE PFIN_PRE_MOVIMIENTO
	   SET IMP_NETO            = ${vlImpNeto}
	 WHERE CVE_GPO_EMPRESA     = ${pCveGpoEmpresa}
	   AND CVE_EMPRESA         = ${pCveEmpresa}
	   AND ID_PRE_MOVIMIENTO   = ${vlIdPreMovto}
	 """
		    
	// Se procesa el movimiento
	def PKG_PROCESADOR_FINANCIERO = new PKG_PROCESADOR_FINANCIERO()
	vlIdMovimiento = PKG_PROCESADOR_FINANCIERO.pProcesaMovimiento(pCveGpoEmpresa, pCveEmpresa, vlIdPreMovto, 'PV','', cFalso, pTxrespuesta, sql);

	println "Id Movimiento: ${vlIdMovimiento}"

        // Se actualiza el identificador del movimiento

	sql.executeUpdate """
           UPDATE PFIN_PRE_MOVIMIENTO 
              SET ID_MOVIMIENTO       = ${vlIdMovimiento}
            WHERE CVE_GPO_EMPRESA     = ${pCveGpoEmpresa}
              AND CVE_EMPRESA         = ${pCveEmpresa}
              AND ID_PRE_MOVIMIENTO   = ${vlIdPreMovto}
	 """

        //Actualiza la informacion de la tabla de amortizacion y de los accesorios con el movimiento generado
        def pTxRespuesta = pActualizaTablaAmortizacion(pCveGpoEmpresa, pCveEmpresa, vlIdMovimiento, sql)


	
    }

    def pActualizaTablaAmortizacion(pCveGpoEmpresa,
             pCveEmpresa,
             pIdMovimiento,
             sql){

        def vlInteresMora;
        def vlIVAInteresMora;
        def vlCapitalPagado;
        def vlInteresPagado;
        def vlIVAInteresPag;
        def vlInteresExtPag;
        def vlIVAIntExtPag;
        def vlImpPagoTardio;
        def vlImpIntMora;
        def vlImpIVAIntMora;
        def vlImpDeuda;
        def vlIdPrestamo;
        def vlNumAmortizacion;
        def vlFValor;
        def vlBPagado;SimpleDateFormat
	def vlImpDeudaMinima;

        // Cursor de conceptos pagados por el movimiento
	// CURSOR DE ACCESORIOS PAGADOS
	def curConceptoPagado = []

	sql.eachRow("""
           SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, A.NUM_PAGO_AMORTIZACION, B.CVE_CONCEPTO, B.IMP_CONCEPTO, C.ID_ACCESORIO
             FROM PFIN_MOVIMIENTO A, PFIN_MOVIMIENTO_DET B, PFIN_CAT_CONCEPTO C
            WHERE A.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}       AND
                  A.CVE_EMPRESA           = ${pCveEmpresa}          AND
                  A.ID_MOVIMIENTO         = ${pIdMovimiento}        AND
                  A.CVE_GPO_EMPRESA       = B.CVE_GPO_EMPRESA       AND
                  A.CVE_EMPRESA           = B.CVE_EMPRESA           AND
                  A.ID_MOVIMIENTO         = B.ID_MOVIMIENTO         AND
                  B.CVE_GPO_EMPRESA       = C.CVE_GPO_EMPRESA       AND
                  B.CVE_EMPRESA           = C.CVE_EMPRESA           AND
                  B.CVE_CONCEPTO          = C.CVE_CONCEPTO          AND
                  C.ID_ACCESORIO NOT IN (1,99)
	"""){
	    curConceptoPagado << it.toRowResult()
	}

	//Recupera la informacion del credito desde el movimiento
	def rowInformacionMovimiento = sql.firstRow(""" 
        SELECT A.ID_PRESTAMO, A.NUM_PAGO_AMORTIZACION, A.F_APLICACION,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'CAPITA'   THEN IMP_CONCEPTO ELSE 0 END) AS IMP_CAPITAL_AMORT_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'INTERE'   THEN IMP_CONCEPTO ELSE 0 END) AS IMP_INTERES_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'IVAINT'   THEN IMP_CONCEPTO ELSE 0 END) AS IMP_IVA_INTERES_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'INTEXT'   THEN IMP_CONCEPTO ELSE 0 END) AS IMP_INTERES_EXTRA_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'IVAINTEX' THEN IMP_CONCEPTO ELSE 0 END) AS IMP_IVA_INTERES_EXTRA_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'PAGOTARD' THEN IMP_CONCEPTO ELSE 0 END) AS IMP_PAGO_TARDIO_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'INTMORA'  THEN IMP_CONCEPTO ELSE 0 END) AS IMP_INTERES_MORA_PAGADO,
               SUM(CASE WHEN B.CVE_CONCEPTO = 'IVAINTMO' THEN IMP_CONCEPTO ELSE 0 END) AS IMP_IVA_INTERES_MORA_PAGADO

          FROM PFIN_MOVIMIENTO A, PFIN_MOVIMIENTO_DET B
         WHERE A.CVE_GPO_EMPRESA = ${pCveGpoEmpresa}  AND
               A.CVE_EMPRESA     = ${pCveEmpresa}     AND
               A.ID_MOVIMIENTO   = ${pIdMovimiento}   AND
               A.CVE_GPO_EMPRESA = B.CVE_GPO_EMPRESA  AND
               A.CVE_EMPRESA     = B.CVE_EMPRESA      AND
               A.ID_MOVIMIENTO   = B.ID_MOVIMIENTO 
         GROUP BY A.ID_PRESTAMO, A.NUM_PAGO_AMORTIZACION, A.F_APLICACION
	""")

	vlIdPrestamo = rowInformacionMovimiento.ID_PRESTAMO
	vlNumAmortizacion = rowInformacionMovimiento.NUM_PAGO_AMORTIZACION
	vlFValor = rowInformacionMovimiento.F_APLICACION
	vlCapitalPagado = rowInformacionMovimiento.IMP_CAPITAL_AMORT_PAGADO
	vlInteresPagado = rowInformacionMovimiento.IMP_INTERES_PAGADO
	vlIVAInteresPag = rowInformacionMovimiento.IMP_IVA_INTERES_PAGADO
	vlInteresExtPag = rowInformacionMovimiento.IMP_INTERES_EXTRA_PAGADO
	vlIVAIntExtPag = rowInformacionMovimiento.IMP_IVA_INTERES_EXTRA_PAGADO
	vlImpPagoTardio = rowInformacionMovimiento.IMP_PAGO_TARDIO_PAGADO
	vlImpIntMora = rowInformacionMovimiento.IMP_INTERES_MORA_PAGADO
	vlImpIVAIntMora = rowInformacionMovimiento.IMP_IVA_INTERES_MORA_PAGADO


	println"""
        UPDATE SIM_TABLA_AMORTIZACION 
           SET IMP_CAPITAL_AMORT_PAGADO     = NVL(IMP_CAPITAL_AMORT_PAGADO,0)      + NVL(${vlCapitalPagado},0),
               IMP_INTERES_PAGADO           = NVL(IMP_INTERES_PAGADO,0)            + NVL(${vlInteresPagado},0),
               IMP_IVA_INTERES_PAGADO       = NVL(IMP_IVA_INTERES_PAGADO,0)        + NVL(${vlIVAInteresPag},0),
               IMP_INTERES_EXTRA_PAGADO     = NVL(IMP_INTERES_EXTRA_PAGADO,0)      + NVL(${vlInteresExtPag},0),
               IMP_IVA_INTERES_EXTRA_PAGADO = NVL(IMP_IVA_INTERES_EXTRA_PAGADO,0)  + NVL(${vlIVAIntExtPag},0),
               IMP_PAGO_TARDIO_PAGADO       = NVL(IMP_PAGO_TARDIO_PAGADO,0)        + NVL(${vlImpPagoTardio},0),
               IMP_INTERES_MORA_PAGADO      = NVL(IMP_INTERES_MORA_PAGADO,0)       + NVL(${vlImpIntMora},0),
               IMP_IVA_INTERES_MORA_PAGADO  = NVL(IMP_IVA_INTERES_MORA_PAGADO,0)   + NVL(${vlImpIVAIntMora},0),
               FECHA_AMORT_PAGO_ULTIMO      = ${vlFValor}
         WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
           AND CVE_EMPRESA           = ${pCveEmpresa}
           AND ID_PRESTAMO           = ${vlIdPrestamo}
           AND NUM_PAGO_AMORTIZACION = ${vlNumAmortizacion}	
	"""
        //Actualiza la informacion de la tabla de amortizacion
	sql.executeUpdate """
        UPDATE SIM_TABLA_AMORTIZACION 
           SET IMP_CAPITAL_AMORT_PAGADO     = NVL(IMP_CAPITAL_AMORT_PAGADO,0)      + NVL(${vlCapitalPagado},0),
               IMP_INTERES_PAGADO           = NVL(IMP_INTERES_PAGADO,0)            + NVL(${vlInteresPagado},0),
               IMP_IVA_INTERES_PAGADO       = NVL(IMP_IVA_INTERES_PAGADO,0)        + NVL(${vlIVAInteresPag},0),
               IMP_INTERES_EXTRA_PAGADO     = NVL(IMP_INTERES_EXTRA_PAGADO,0)      + NVL(${vlInteresExtPag},0),
               IMP_IVA_INTERES_EXTRA_PAGADO = NVL(IMP_IVA_INTERES_EXTRA_PAGADO,0)  + NVL(${vlIVAIntExtPag},0),
               IMP_PAGO_TARDIO_PAGADO       = NVL(IMP_PAGO_TARDIO_PAGADO,0)        + NVL(${vlImpPagoTardio},0),
               IMP_INTERES_MORA_PAGADO      = NVL(IMP_INTERES_MORA_PAGADO,0)       + NVL(${vlImpIntMora},0),
               IMP_IVA_INTERES_MORA_PAGADO  = NVL(IMP_IVA_INTERES_MORA_PAGADO,0)   + NVL(${vlImpIVAIntMora},0),
               FECHA_AMORT_PAGO_ULTIMO      = ${vlFValor}
         WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
           AND CVE_EMPRESA           = ${pCveEmpresa}
           AND ID_PRESTAMO           = ${vlIdPrestamo}
           AND NUM_PAGO_AMORTIZACION = ${vlNumAmortizacion}	
	"""

	curConceptoPagado.each{ vlBufConceptoPagado ->
		sql.executeUpdate """
		    UPDATE SIM_TABLA_AMORT_ACCESORIO
		       SET IMP_ACCESORIO_PAGADO  = NVL(IMP_ACCESORIO_PAGADO,0) + ${vlBufConceptoPagado.IMP_CONCEPTO}
		     WHERE CVE_GPO_EMPRESA       = ${vlBufConceptoPagado.CVE_GPO_EMPRESA}       AND
		           CVE_EMPRESA           = ${vlBufConceptoPagado.CVE_EMPRESA}           AND
		           ID_PRESTAMO           = ${vlBufConceptoPagado.ID_PRESTAMO}           AND
		           NUM_PAGO_AMORTIZACION = ${vlBufConceptoPagado.NUM_PAGO_AMORTIZACION} AND
		           ID_ACCESORIO          = ${vlBufConceptoPagado.ID_ACCESORIO}
		"""
	}

        //Recupera el saldo de la amortizacion
	def rowSaldoAmortizacion = sql.firstRow(""" 
		SELECT SUM(IMP_NETO) IMP_NETO
		  FROM V_SIM_TABLA_AMORT_CONCEPTO 
		 WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa} AND
		       CVE_EMPRESA           = ${pCveEmpresa}    AND
		       ID_PRESTAMO           = ${vlIdPrestamo}   AND
		       NUM_PAGO_AMORTIZACION = ${vlNumAmortizacion}
	""")
	vlImpDeuda = rowSaldoAmortizacion.IMP_NETO

        // Recupera el valor de la deuda minima
	def rowDeudaMinima = sql.firstRow(""" 
		SELECT IMP_DEUDA_MINIMA
		  FROM SIM_PARAMETRO_GLOBAL 
		 WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa} AND
		       CVE_EMPRESA           = ${pCveEmpresa}
	""")
	vlImpDeudaMinima = rowDeudaMinima.IMP_DEUDA_MINIMA

        // Verifica si se liquido el pago total del credito
        if (vlImpDeuda > -vlImpDeudaMinima){
           vlBPagado = cVerdadero 
	}
        else{
           vlBPagado = cFalso
        }

	//Actualiza la informacion de pago puntual y pago completo en la tabla de amortizacion 
	//VERIFICAR QUE LA VALIDACION DE LA FECHA SEA CORRECTA
	sql.executeUpdate """
		UPDATE SIM_TABLA_AMORTIZACION 
		   SET B_PAGO_PUNTUAL        = CASE WHEN FECHA_AMORTIZACION >= ${vlFValor} AND ${vlBPagado} = ${cVerdadero} THEN ${cVerdadero}
                                            	ELSE ${cFalso} END,
		       B_PAGADO              = ${vlBPagado},
		       IMP_PAGO_PAGADO       = IMP_CAPITAL_AMORT_PAGADO + IMP_INTERES_PAGADO + IMP_INTERES_EXTRA_PAGADO + 
		                               IMP_IVA_INTERES_PAGADO   + IMP_IVA_INTERES_EXTRA_PAGADO               
		 WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
		   AND CVE_EMPRESA           = ${pCveEmpresa}
		   AND ID_PRESTAMO           = ${vlIdPrestamo}
		   AND NUM_PAGO_AMORTIZACION = ${vlNumAmortizacion}
	"""
    }

    def pGeneraTablaAmortizacion(pCveGpoEmpresa,
             pCveEmpresa,
             pIdPrestamo,
	     pTxRespuesta,
             sql){

	    def vlIdCliente
	    def vlIdGrupo
	    def vlIdSucursal
	    def vlCveMetodo
	    def vlPlazo
	    def vlTasaInteres
	    def vlTasaIVA
	    def vlFInicioEntrega
	    def vlMontoInicial
	    def vlFReal
	    def vlFEntrega
	    def vlIdPeriodicidad
	    Integer vlDiasPeriodicidad
	    def vlFechaAmort
	    def vlNumPagos
	    def i = 0

            def V_CVE_GPO_EMPRESA   = pCveGpoEmpresa
            def V_CVE_EMPRESA       = pCveEmpresa
            def V_ID_PRESTAMO       = pIdPrestamo

	    def V_TASA_INTERES
            def V_B_PAGO_PUNTUAL               
	    def V_IMP_INTERES_PAGADO            
	    def V_IMP_INTERES_EXTRA_PAGADO      
	    def V_IMP_CAPITAL_AMORT_PAGADO     
	    def V_IMP_PAGO_PAGADO               
	    def V_IMP_IVA_INTERES_PAGADO        
	    def V_IMP_IVA_INTERES_EXTRA_PAGADO 
	    def V_B_PAGADO                      
	    def V_FECHA_AMORT_PAGO_ULTIMO        
	    def V_IMP_CAPITAL_AMORT_PREPAGO    
	    def V_NUM_DIA_ATRASO 
	    def V_NUM_PAGO_AMORTIZACION 
	    def V_IMP_SALDO_INICIAL 
	    def V_IMP_INTERES_EXTRA   
	    def V_IMP_IVA_INTERES_EXTRA
  	    def V_IMP_CAPITAL_AMORT
	    def V_IMP_INTERES
	    def V_IMP_IVA_INTERES
	    def V_IMP_SALDO_FINAL
	    def V_FECHA_AMORTIZACION
       

	    //Cursor que obtiene los accesorios que tiene relacionados el préstamo
	    def curAccesorios = []
	    sql.eachRow("""
	
			SELECT C.ID_CARGO_COMISION, C.ID_FORMA_APLICACION, C.VALOR VALOR_CARGO, U.VALOR VALOR_UNIDAD, 
			       PC.DIAS DIAS_CARGO, PP.DIAS DIAS_PRODUCTO, CA.TASA_IVA 
			  FROM SIM_PRESTAMO P, SIM_PRESTAMO_CARGO_COMISION C, SIM_CAT_ACCESORIO CA, SIM_CAT_PERIODICIDAD PC, 
			       SIM_CAT_PERIODICIDAD PP, SIM_CAT_UNIDAD U
			  WHERE P.CVE_GPO_EMPRESA 	         = ${pCveGpoEmpresa}
			    AND P.CVE_EMPRESA     	         = ${pCveEmpresa}
			    AND P.ID_PRESTAMO     	         = ${pIdPrestamo}
			    AND P.CVE_GPO_EMPRESA 	         = C.CVE_GPO_EMPRESA
			    AND P.CVE_EMPRESA     	         = C.CVE_EMPRESA
			    AND P.ID_PRESTAMO     	         = C.ID_PRESTAMO
			    AND C.CVE_GPO_EMPRESA 	         = CA.CVE_GPO_EMPRESA
			    AND C.CVE_EMPRESA     	         = CA.CVE_EMPRESA
			    AND C.ID_CARGO_COMISION              = CA.ID_ACCESORIO
			    AND CA.CVE_TIPO_ACCESORIO            = 'CARGO_COMISION'
			    AND C.CVE_GPO_EMPRESA 	         = U.CVE_GPO_EMPRESA(+)
			    AND C.CVE_EMPRESA     	         = U.CVE_EMPRESA(+)
			    AND C.ID_UNIDAD                      = U.ID_UNIDAD(+)
			    AND C.CVE_GPO_EMPRESA 	         = PC.CVE_GPO_EMPRESA(+)
			    AND C.CVE_EMPRESA     	         = PC.CVE_EMPRESA(+)
			    AND C.ID_PERIODICIDAD                = PC.ID_PERIODICIDAD(+)
			    AND P.CVE_GPO_EMPRESA 	         = PP.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	         = PP.CVE_EMPRESA(+)
			    AND P.ID_PERIODICIDAD_PRODUCTO       = PP.ID_PERIODICIDAD(+)
			    AND C.ID_FORMA_APLICACION            NOT IN (1,2)
		""") {
		  curAccesorios << it.toRowResult()
	    }

	    def rowContarPagos = sql.firstRow(""" 
		     SELECT COUNT(1) NUM_PAGOS
		        FROM PFIN_MOVIMIENTO
		       WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
		             CVE_EMPRESA     = ${pCveEmpresa}    AND
		             ID_PRESTAMO     = ${pIdPrestamo}    AND
		             CVE_OPERACION   = ${cPagoPrestamo}  AND
		             SIT_MOVIMIENTO  <> ${cSitCancelada}
	    """)
	    vlNumPagos = rowContarPagos.NUM_PAGOS
	    println "Num pagos: ${vlNumPagos}"

            if( vlNumPagos < 0 ){ //CORREGIR VALIDACION
		pTxRespuesta = 'No se actualiza la tabla de amortizacion por que ya existen pagos para este prestamo'
		println pTxRespuesta
	    }else{

		    // Se obtienen los datos genéricos del préstamo
		    def rowDatosPrestamo = sql.firstRow(""" 
			    SELECT P.ID_CLIENTE, P.ID_GRUPO, P.FECHA_ENTREGA, P.CVE_METODO, S.ID_SUCURSAL, C.TASA_IVA,
			    		   P.PLAZO, NVL(P.FECHA_REAL, P.FECHA_ENTREGA) FECHA_REAL, P.ID_PERIODICIDAD_PRODUCTO, 
					   P.FECHA_ENTREGA, PP.DIAS
			      FROM SIM_PRESTAMO P, SIM_PRODUCTO_SUCURSAL S, SIM_CAT_SUCURSAL C, SIM_CAT_PERIODICIDAD PP
			     WHERE P.CVE_GPO_EMPRESA 	         = ${pCveGpoEmpresa}
			       AND P.CVE_EMPRESA     	         = ${pCveEmpresa}
			       AND P.ID_PRESTAMO     	         = ${pIdPrestamo}
			       AND P.CVE_GPO_EMPRESA	         = S.CVE_GPO_EMPRESA
			       AND P.CVE_EMPRESA    	         = S.CVE_EMPRESA
			       AND P.ID_PRODUCTO                 = S.ID_PRODUCTO
			       AND P.ID_SUCURSAL                 = S.ID_SUCURSAL        
			       AND S.CVE_GPO_EMPRESA	         = C.CVE_GPO_EMPRESA
			       AND S.CVE_EMPRESA    	         = C.CVE_EMPRESA
			       AND S.ID_SUCURSAL 	         = C.ID_SUCURSAL
			       AND P.CVE_GPO_EMPRESA 	         = PP.CVE_GPO_EMPRESA(+)
			       AND P.CVE_EMPRESA     	         = PP.CVE_EMPRESA(+)
			       AND P.ID_PERIODICIDAD_PRODUCTO    = PP.ID_PERIODICIDAD(+)
		    """)

			vlIdCliente = rowDatosPrestamo.ID_CLIENTE
			vlIdGrupo = rowDatosPrestamo.ID_GRUPO
			vlFInicioEntrega = rowDatosPrestamo.FECHA_ENTREGA
			vlCveMetodo = rowDatosPrestamo.CVE_METODO
			vlIdSucursal = rowDatosPrestamo.ID_SUCURSAL
			vlTasaIVA = rowDatosPrestamo.TASA_IVA
			vlPlazo = rowDatosPrestamo.PLAZO
			vlFReal = rowDatosPrestamo.FECHA_REAL
			vlIdPeriodicidad = rowDatosPrestamo.ID_PERIODICIDAD_PRODUCTO
			vlFEntrega = rowDatosPrestamo.FECHA_ENTREGA
			vlDiasPeriodicidad = rowDatosPrestamo.DIAS

			println rowDatosPrestamo
			
			V_TASA_INTERES = DameTasaAmort(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, vlTasaIVA, pTxRespuesta, sql)

			println "TASA_INTERES: ${V_TASA_INTERES}"

			// se borra la tabla de accesorios de amortización
			sql.execute """
				DELETE SIM_TABLA_AMORT_ACCESORIO
				 WHERE CVE_GPO_EMPRESA 	= ${pCveGpoEmpresa}
				   AND CVE_EMPRESA     	= ${pCveEmpresa}
				   AND ID_PRESTAMO     	= ${pIdPrestamo}
			"""

			// se borra la tabla de amortización
			sql.execute """
				DELETE SIM_TABLA_AMORTIZACION
				 WHERE CVE_GPO_EMPRESA 	= ${pCveGpoEmpresa}
				   AND CVE_EMPRESA     	= ${pCveEmpresa}
				   AND ID_PRESTAMO     	= ${pIdPrestamo}
			"""

			// Inicializa variables
			V_B_PAGO_PUNTUAL                = cFalso
			V_IMP_INTERES_PAGADO            = 0
			V_IMP_INTERES_EXTRA_PAGADO      = 0
			V_IMP_CAPITAL_AMORT_PAGADO      = 0
			V_IMP_PAGO_PAGADO               = 0
			V_IMP_IVA_INTERES_PAGADO        = 0
			V_IMP_IVA_INTERES_EXTRA_PAGADO  = 0
			V_B_PAGADO                      = cFalso
			V_FECHA_AMORT_PAGO_ULTIMO        
			V_IMP_CAPITAL_AMORT_PREPAGO     = 0
			V_NUM_DIA_ATRASO                = 0

			while ( i < vlPlazo ) {
				i = i + 1
				V_NUM_PAGO_AMORTIZACION = i
				if (i == 1){
					// Se obtiene el monto inicial la primera vez
					def montoAutorizado = DameMontoAutorizado (pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, vlIdCliente, pTxRespuesta,sql)
					println "montoAutorizado: ${montoAutorizado}"
					def cargoInicial = DameCargoInicial(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, pTxRespuesta, sql)
					println "cargoInicial: ${cargoInicial}"
					V_IMP_SALDO_INICIAL = montoAutorizado + cargoInicial
					println "saldo inicial: ${V_IMP_SALDO_INICIAL}"

   				        // si la periodicidad es Catorcenal o Semanal y la fecha de entrega es diferente a la 
				        // real se calculan intereses extra
				        if ((vlIdPeriodicidad==7 || vlIdPeriodicidad==8) && vlFEntrega != vlFReal){
						if (vlCveMetodo !='05' || vlCveMetodo != '06'){
							println "fecha de entrega es diferente a la real"

				                        V_IMP_INTERES_EXTRA = DameInteresExtra(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, V_IMP_SALDO_INICIAL,vlTasaIVA, pTxRespuesta,sql) / (1 + vlTasaIVA/100)
							println "Interes extra: ${V_IMP_INTERES_EXTRA}"
							V_IMP_IVA_INTERES_EXTRA = V_IMP_INTERES_EXTRA * (vlTasaIVA / 100)
							println "V_IMP_IVA_INTERES_EXTRA: ${V_IMP_IVA_INTERES_EXTRA}"

						}else{
							V_IMP_SALDO_INICIAL = V_IMP_SALDO_INICIAL + 
                                                        (DameInteresExtra(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, V_IMP_SALDO_INICIAL,
                                                                          vlTasaIVA,pTxRespuesta,sql) * vlPlazo)
							println "V_IMP_SALDO_INICIAL: ${V_IMP_SALDO_INICIAL}"
				                        V_IMP_INTERES_EXTRA     = 0
                            				V_IMP_IVA_INTERES_EXTRA = 0

						}
					}else{
			                        V_IMP_INTERES_EXTRA     = 0
                       				V_IMP_IVA_INTERES_EXTRA = 0
					}
		                        //Se guarda el monto para cálculos posteriores
					vlMontoInicial                 = V_IMP_SALDO_INICIAL
					V_IMP_CAPITAL_AMORT            = V_IMP_SALDO_INICIAL / vlPlazo
					V_IMP_INTERES                  = V_IMP_SALDO_INICIAL * V_TASA_INTERES / (1+vlTasaIVA / 100)
					V_IMP_IVA_INTERES              = V_IMP_INTERES * (vlTasaIVA / 100)
					V_IMP_SALDO_FINAL              = V_IMP_SALDO_INICIAL - V_IMP_CAPITAL_AMORT
					V_FECHA_AMORTIZACION           = vlFReal
					vlFechaAmort                   = vlFReal



				}else{
					V_IMP_SALDO_INICIAL     = V_IMP_SALDO_FINAL
					V_IMP_SALDO_FINAL       = V_IMP_SALDO_INICIAL - V_IMP_CAPITAL_AMORT

				        if (vlCveMetodo != '01'){
				            V_IMP_INTERES       = V_IMP_SALDO_INICIAL * V_TASA_INTERES / (1 + vlTasaIVA / 100)
				            V_IMP_IVA_INTERES   = V_IMP_INTERES * (vlTasaIVA / 100)
				        }


				}
				// Se calcula la fecha de amortización
				vlFechaAmort  = vlFechaAmort + vlDiasPeriodicidad

				V_FECHA_AMORTIZACION = DameFechaValida(pCveGpoEmpresa, pCveEmpresa, vlFechaAmort, 'MX', pTxRespuesta,sql)
				println "V_FECHA_AMORTIZACION: ${V_FECHA_AMORTIZACION}"
			}




	    }


    }

	def DameTasaAmort(pCveGpoEmpresa,
                           pCveEmpresa,
                           pIdPrestamo,
                           pTasaIva,
                           pTxRespuesta,
			   sql){

		def vlTasaInteres
		def rowTasaInteres = sql.firstRow(""" 
		       SELECT 	ROUND(DECODE(P.TIPO_TASA,'No indexada', P.VALOR_TASA, T.VALOR)
				* (1 + ${pTasaIva} / 100) / 100
				/ DECODE(P.TIPO_TASA,'No indexada', PT.DIAS, PTI.DIAS)
   			        * PP.DIAS
				,20) AS TASA_INTERES
			   FROM SIM_PRESTAMO P, SIM_CAT_PERIODICIDAD PT, SIM_CAT_PERIODICIDAD PTI,
				SIM_CAT_PERIODICIDAD PP, SIM_CAT_TASA_REFER_DETALLE T, SIM_CAT_TASA_REFERENCIA TR
			  WHERE P.CVE_GPO_EMPRESA 	        = ${pCveGpoEmpresa}
			    AND P.CVE_EMPRESA     	        = ${pCveEmpresa}
			    AND P.ID_PRESTAMO     	        = ${pIdPrestamo}
			    AND P.CVE_GPO_EMPRESA 	        = PT.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = PT.CVE_EMPRESA(+)
			    AND P.ID_PERIODICIDAD_TASA          = PT.ID_PERIODICIDAD(+)
			    AND P.CVE_GPO_EMPRESA 	        = PP.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = PP.CVE_EMPRESA(+)
			    AND P.ID_PERIODICIDAD_PRODUCTO      = PP.ID_PERIODICIDAD(+)
			    AND P.CVE_GPO_EMPRESA 	        = TR.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = TR.CVE_EMPRESA(+)
			    AND P.ID_TASA_REFERENCIA            = TR.ID_TASA_REFERENCIA(+)
			    AND P.CVE_GPO_EMPRESA 	        = T.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = T.CVE_EMPRESA(+)
			    AND P.ID_TASA_REFERENCIA            = T.ID_TASA_REFERENCIA(+)
			    AND P.FECHA_TASA_REFERENCIA         = T.FECHA_PUBLICACION(+)
			    AND TR.CVE_GPO_EMPRESA 	        = PTI.CVE_GPO_EMPRESA(+)
			    AND TR.CVE_EMPRESA     	        = PTI.CVE_EMPRESA(+)
			    AND TR.ID_PERIODICIDAD              = PTI.ID_PERIODICIDAD(+)
		""")
		
		vlTasaInteres = rowTasaInteres.TASA_INTERES

	}

	def DameMontoAutorizado(pCveGpoEmpresa,
                                pCveEmpresa,
                                pIdPrestamo,
                                pIdCliente,
                                pTxrespuesta,
				sql){
		def vlMontoCliente

		def rowMontoAutorizado = sql.firstRow(""" 
		       SELECT NVL(MONTO_AUTORIZADO, MONTO_SOLICITADO) MONTO_AUTORIZADO
			  FROM SIM_CLIENTE_MONTO
			 WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
			   AND CVE_EMPRESA     = ${pCveEmpresa}
			   AND ID_PRESTAMO     = ${pIdPrestamo}
			   AND ID_CLIENTE      = ${pIdCliente}
			""")
		vlMontoCliente = rowMontoAutorizado.MONTO_AUTORIZADO

	}

	def DameCargoInicial(pCveGpoEmpresa,
                              pCveEmpresa,
                              pIdPrestamo,
                              pTxrespuesta,
			      sql){
		def vlCargoInicial

		def rowMontoAutorizado = sql.firstRow(""" 
			SELECT SUM(NVL(C.CARGO_INICIAL, C.PORCENTAJE_MONTO / 100 * M.MONTO_AUTORIZADO) ) CARGO_INICIAL
			  FROM SIM_PRESTAMO_CARGO_COMISION C, SIM_CLIENTE_MONTO M
			 WHERE C.CVE_GPO_EMPRESA     = ${pCveGpoEmpresa}
			   AND C.CVE_EMPRESA         = ${pCveEmpresa}
			   AND C.ID_PRESTAMO         = ${pIdPrestamo}
			   AND C.ID_FORMA_APLICACION = 1
			   AND M.CVE_GPO_EMPRESA     = C.CVE_GPO_EMPRESA
			   AND M.CVE_EMPRESA         = C.CVE_EMPRESA
			   AND M.ID_PRESTAMO         = C.ID_PRESTAMO
			""")
		vlCargoInicial = rowMontoAutorizado.CARGO_INICIAL

	}

	def DameInteresExtra(pCveGpoEmpresa,
                              pCveEmpresa,
                              pIdPrestamo,
                              pSaldoInicial,
                              pTasaIva,
                              pTxRespuesta,
			      sql){

		def vlImpInteresExtra

		// Se obtiene el valor de la tasa
		def rowInteresExtra = sql.firstRow(""" 
			SELECT 	ROUND(${pSaldoInicial} * (NVL(P.FECHA_REAL, P.FECHA_ENTREGA) - P.FECHA_ENTREGA) / DECODE(P.TIPO_TASA, 'No indexada',
			  PT.DIAS, PTI.DIAS)*(DECODE(P.TIPO_TASA, 'No indexada', P.VALOR_TASA, T.VALOR) * (1 + ${pTasaIva} / 100) ) /100 /P.PLAZO, 10)
			  VALOR_INTERES_EXTRA
			  FROM SIM_PRESTAMO P, SIM_CAT_PERIODICIDAD PT, SIM_CAT_PERIODICIDAD PTI, SIM_CAT_PERIODICIDAD PP, 
			       SIM_CAT_TASA_REFER_DETALLE T, SIM_CAT_TASA_REFERENCIA TR
			 WHERE  P.CVE_GPO_EMPRESA 	        = ${pCveGpoEmpresa}
			    AND P.CVE_EMPRESA     	        = ${pCveEmpresa}
			    AND P.ID_PRESTAMO     	        = ${pIdPrestamo}
			    AND P.CVE_GPO_EMPRESA 	        = PT.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = PT.CVE_EMPRESA(+)
			    AND P.ID_PERIODICIDAD_TASA          = PT.ID_PERIODICIDAD(+)
			    AND P.CVE_GPO_EMPRESA 	        = PP.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = PP.CVE_EMPRESA(+)
			    AND P.ID_PERIODICIDAD_PRODUCTO      = PP.ID_PERIODICIDAD(+)
			    AND P.CVE_GPO_EMPRESA 	        = TR.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = TR.CVE_EMPRESA(+)
			    AND P.ID_TASA_REFERENCIA            = TR.ID_TASA_REFERENCIA(+)
			    AND P.CVE_GPO_EMPRESA 	        = T.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	        = T.CVE_EMPRESA(+)
			    AND P.ID_TASA_REFERENCIA            = T.ID_TASA_REFERENCIA(+)
			    AND P.FECHA_TASA_REFERENCIA         = T.FECHA_PUBLICACION(+)
			    AND TR.CVE_GPO_EMPRESA 	        = PTI.CVE_GPO_EMPRESA(+)
			    AND TR.CVE_EMPRESA     	        = PTI.CVE_EMPRESA(+)
			    AND TR.ID_PERIODICIDAD              = PTI.ID_PERIODICIDAD(+)
		""")
		vlImpInteresExtra = rowInteresExtra.VALOR_INTERES_EXTRA
	}

	def DameFechaValida (pCveGpoEmpresa,
                              pCveEmpresa,
                              pFecha,
                              pCvePais,
                              pTxRespuesta,
			      sql){

	        def vlFechaOK
        	def vlFechaTemp
        	def vlFechaTempFormato
        	def vlBufParametro
        	def vlDia         
        	def lbFechaValida
		def vlFFind
		def vlFechaEncontrada = cFalso

		// Se obtienen los datos de la tabla de parámetros
		vlBufParametro = sql.firstRow(""" 
			SELECT *
			  FROM PFIN_PARAMETRO
			 WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
			   AND CVE_EMPRESA     = ${pCveEmpresa}
			   AND CVE_MEDIO       = 'SYSTEM'
		""")

		//FORMATO DE LAS FECHAS
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf = new SimpleDateFormat("dd-MM-yyyy");


		vlFechaTemp = pFecha
		while (vlFechaEncontrada == cFalso){
			def esDiaFestivo = cFalso

			vlFechaTempFormato = vlFechaTemp
			vlFechaTempFormato = sdf.format(vlFechaTempFormato)

			//Si existe la fecha como fecha válida regresa verdadero de otro modo falso
			sql.eachRow(""" 
			    SELECT F_DIA_FESTIVO
			      FROM PFIN_DIA_FESTIVO
			     WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
			       AND CVE_EMPRESA     = ${pCveEmpresa}
			       AND CVE_PAIS        = ${pCvePais}
			       AND F_DIA_FESTIVO   = TO_DATE(${vlFechaTempFormato},'DD-MM-YYYY')
			"""){
				esDiaFestivo = cVerdadero
			}

			//OBTIENE EL DIA DE PAGO
			//0 = Domingo, 7 = Sabado	
			vlDia = vlFechaTemp.getDay()
			if (vlDia == 0 || vlDia == 6 || esDiaFestivo == cVerdadero){
				if (vlBufParametro.B_OPERA_DOMINGO == 'V' && vlDia == '0'){
					vlFechaEncontrada = cVerdadero
				}else if(vlBufParametro.B_OPERA_SABADO =='V' && vlDia == '6'){
					vlFechaEncontrada = cVerdadero
				}else if(vlBufParametro.B_OPERA_DIA_FESTIVO =='V' &&  esDiaFestivo == cVerdadero){
					vlFechaEncontrada = cVerdadero
				}else{
					vlFechaTemp = vlFechaTemp + 1
				}
			}else{
				vlFechaEncontrada = cVerdadero
			}

		}// END WHILE
		return	vlFechaTempFormato	

	}


}

