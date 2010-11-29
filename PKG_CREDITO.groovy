//PKG_CREDITO
class PKG_CREDITO {

    //PARAMETROS DE ENTRADA
    def cSitCancelada = 'CA'
    def cFalso = 'F'
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

	println "INICIA pAplicaPagoCredito -------------------------"
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

	
    }
}


