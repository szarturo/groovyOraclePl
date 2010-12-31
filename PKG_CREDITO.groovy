//PKG_CREDITO
import java.text.SimpleDateFormat;

class PKG_CREDITO {

	//VARIABLES LOCALES
	def cSitCancelada = 'CA'
	def cFalso = 'F'
	def cVerdadero = 'V'
	def cDivisaPeso = 'MXP'
	def cPagoPrestamo = 'CRPAGOPRES' //PAGO DE PRESTAMO
	def vgFechaSistema

	String toString(){"PKG_CREDITO"}

	def AsignaFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql){
	    def row = sql.firstRow("""
		       SELECT   F_MEDIO
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

		//FALTA VALIDAR LOS MOVIMIENTOS POSTERIORES

		//OBTIENE LAS AMORTIZACIONES PENDIENTES DE PAGO
		//¿COMO MANEJA EL SISTEMA LOS PAGOS ADELANTADOS
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

		// Ejecuta el pago de cada amortizacion mientras exista una amortizacion pendiente de pagar y el cliente 
		// tenga saldo en su cuenta	
		curAmortizacionesPendientes.each{ vlBufAmortizaciones ->   

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
		
			if (vlImpSaldo > 0){
				vlResultado = pAplicaPagoCreditoPorAmort(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo,
					 vlBufAmortizaciones.NUM_PAGO_AMORTIZACION,vlImpSaldo, vlIdCuenta, pCveUsuario, pFValor, pTxrespuesta,sql)
			}else{
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

		//Se obtiene el id del premovimiento
		def rowIdPremovimiento = sql.firstRow("SELECT SQ01_PFIN_PRE_MOVIMIENTO.nextval as ID_PREMOVIMIENTO FROM DUAL")
		vlIdPreMovto = rowIdPremovimiento.ID_PREMOVIMIENTO
	
		//Se genera el premovimiento      
		def PKG_PROCESOS = new PKG_PROCESOS()

		PKG_PROCESOS.pGeneraPreMovto(pCveGpoEmpresa,pCveEmpresa,vlIdPreMovto,vgFechaSistema,pIdCuenta,pIdPrestamo,
				  cDivisaPeso,cPagoPrestamo,vlImpNeto,'PRESTAMO', 'PRESTAMO', 'Pago de préstamo',0,
				  pCveUsuario,pFValor,pNumAmortizacion,pTxrespuesta,sql)

		def curDebe = []
		sql.eachRow("""
		 SELECT DECODE(A.CVE_CONCEPTO, 'CAPITA', D.ORDEN_CAPITAL, D.ORDEN_ACCESORIO)
			* 100 + DECODE(A.CVE_CONCEPTO, 'CAPITA',0,B.ORDEN) AS ID_ORDEN, 
			A.CVE_CONCEPTO, 
			INITCAP(C.DESC_LARGA) DESCRIPCION, 
			--NO SE NECESITA EL GROUP, POR LO TANTO NO SE NECESITA EL SUM
			ABS(ROUND(SUM(A.IMP_NETO),2)) AS IMP_NETO
		   FROM V_SIM_TABLA_AMORT_CONCEPTO A, 
			--SE OBTIENE LA DESCRIPCION LARGA DEL CONCEPTO
			PFIN_CAT_CONCEPTO C, 
			--SE OBTIENE LA FORMA DE DISTRIBUCION
			--1 Accesorios y Capital
			--2 Capital y Accesorios
			SIM_PRESTAMO P, SIM_CAT_FORMA_DISTRIBUCION D, 
			--OBTIENE EL ORDEN DE LOS ACCESORIOS PARA EL PRESTAMO
			SIM_PRESTAMO_ACCESORIO B
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
		--NO SE NECESITAN GROUP Y HAVING
		GROUP BY DECODE(A.CVE_CONCEPTO, 'CAPITA', D.ORDEN_CAPITAL, D.ORDEN_ACCESORIO)
			 * 100 + DECODE(A.CVE_CONCEPTO, 'CAPITA',0,B.ORDEN), A.CVE_CONCEPTO, INITCAP(C.DESC_LARGA)
		HAVING ROUND(SUM(IMP_NETO),2) < 0
		ORDER BY DECODE(A.CVE_CONCEPTO, 'CAPITA', D.ORDEN_CAPITAL, D.ORDEN_ACCESORIO)
			 * 100 + DECODE(A.CVE_CONCEPTO, 'CAPITA', 0, B.ORDEN)
		"""){
		    curDebe << it.toRowResult()
		}
				  
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

			}
	    	}

		// Cuando es el metodo seis y aun sobra saldo, es necesario pagar capital adelantado y recalcular la tabla de 
		// amortizacion para los pagos subsecuentes
		// Solo se puede adelantar hasta el capital pendiente de pago
		if (vlImpSaldo > 0 && vlCveMetodo == '06' ){
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
		vlIdMovimiento = PKG_PROCESADOR_FINANCIERO.pProcesaMovimiento(pCveGpoEmpresa, pCveEmpresa, vlIdPreMovto, 'PV','',
				 cFalso, pTxrespuesta, sql);

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
			  --EXCLUYE LOS IVA'S Y EL CAPITAL
		          C.ID_ACCESORIO NOT IN (1,99)
		"""){
		    curConceptoPagado << it.toRowResult()
		}

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
			   SET B_PAGO_PUNTUAL        = CASE WHEN FECHA_AMORTIZACION >= ${vlFValor} AND ${vlBPagado} = ${cVerdadero} 
							THEN ${cVerdadero}
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
		def V_IMP_PAGO
		def V_IMP_PAGO_TARDIO
		def V_IMP_PAGO_TARDIO_PAGADO
		def V_IMP_INTERES_MORA
		def V_IMP_INTERES_MORA_PAGADO
		def V_IMP_IVA_INTERES_MORA
		def V_IMP_IVA_INTERES_MORA_PAGADO
		def V_F_VALOR_CALCULO
		def V_F_INI_AMORTIZACION
		def V_IMP_INTERES_DEV_X_DIA
	       
		//OBTIENE LA FECHA DEL MEDIO, LA FECHA QUE TIENE ASIGNADA EL SIM
		//FUNCION REPETIDA EN LOS 3 PAQUETES
		vgFechaSistema = AsignaFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql)

		//Cursor que obtiene los accesorios que tiene relacionados el préstamo
		//OBTIENE LOS CARGOS COMISIONES ASIGNADOS AL PRESTAMO
		//EXCLUYE LOS CARGOS COMISIONES QUE LA FORMA DE APLICACION ES CARGO INICIAL Y DEPOSITO INICIAL
		//LA FORMA DE APLICACION QUE TOMA EN CUENTA SON: PERIODICAMENTE DEL MONTO PRESTADO, PERIODICAMENTE DEPENDIENDO DEL SALDO A LA FECHA
		//DE CALCULO  Y PERIODICAMENTE CARGO FIJO
		//LA PERIODICIDAD DEL PRODUCTO Y DEL ACCESORIO PUEDEN SER VALORES DISTINTOS, POR ESE MOTIVO SE NECESITAN LOS DATOS
		// PC.DIAS DIAS_CARGO Y PP.DIAS DIAS_PRODUCTO 
		//¿QUE SIGNIFICADO TIENE SIM_CAT_ACCESORIO.TASA_IVA?
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
		    
		//CUENTA LOS PAGOS O TRANSACCIONES REALIZADAS AL PRESTAMO
		//BUSCA LAS TRANSACCIONES IGUALES A CRPAGOPRES (PAGO DE PRESTAMO) Y QUE NO HAYAN SIDO CANCELADAS
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
		    
		//VERIFICA SI EXISTEN PAGOS REALIZADOS AL PRESTAMO
		if( vlNumPagos > 0 ){ 
			//EXISTEN PAGOS REALIZADOS AL PRESTAMO
			pTxRespuesta = 'No se actualiza la tabla de amortizacion por que ya existen pagos para este prestamo'
		}else{
			//LA TABLA DE AMORTIZACION SE GENERA CUANDO NO EXISTEN PAGOS REALIZADOS AL PRESTAMO
			//SE OBTIENE EL CLIENTE, GRUPO, FECHA DE INICIO DE ENTREGA, CLAVE DE METODO DEL CALCULO, SUCURSAL, IVA DE LA TASA
			//PLAZO, FECHA REAL, DIAS PERIODICIDAD, FECHA DE ENTREGA
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
	
			//OBTIENE LA TASA DE INTERES (ESTE VALOR ES CALCULADO MAS EL IVA DE LA TASA DE INTERES)
			V_TASA_INTERES = DameTasaAmort(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, vlTasaIVA, pTxRespuesta, sql)

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

			//ITERA EL PRESTAMO DE ACUERDO AL PLAZO DEFINIDO
			while ( i < vlPlazo ) {
				i = i + 1
				V_NUM_PAGO_AMORTIZACION = i
				if (i == 1){
					//NUMERO DE PLAZO IGUAL A UNO
					// Se obtiene el monto inicial la primera vez
					def montoAutorizado = DameMontoAutorizado (pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, 
								vlIdCliente, pTxRespuesta,sql)
					def cargoInicial = DameCargoInicial(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, pTxRespuesta, sql)
					//EL SALDO INICIAL ES IGUAL AL MONTO AUTORIZADO MAS EL CARGO INICIAL
					V_IMP_SALDO_INICIAL = montoAutorizado + cargoInicial

					// si la periodicidad es Catorcenal o Semanal y la fecha de entrega es diferente a la 
					// real se calculan intereses extra
					if ((vlIdPeriodicidad==7 || vlIdPeriodicidad==8) && vlFEntrega != vlFReal){
						//VERIFICAR EN QUE METODOS APLICA EL CAMBIO DE FECHA
						//TENGO ENTENDIDO QUE SOLO APLICA AL METODO UNO
						if (vlCveMetodo !='05' || vlCveMetodo != '06'){
							//EL METODO DE CALCULO NO ES 5 NI 6
							//OBTIENE EL INTERES EXTRA,A ESTE VALOR SE ELIMINA EL IVA
						        V_IMP_INTERES_EXTRA = DameInteresExtra(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo,
								 V_IMP_SALDO_INICIAL,vlTasaIVA, pTxRespuesta,sql) / (1 + vlTasaIVA/100)
							//CALCULO DEL IVA DEL INTERES EXTRA
							V_IMP_IVA_INTERES_EXTRA = V_IMP_INTERES_EXTRA * (vlTasaIVA / 100)

						}else{
							//SI EL METODO DE CALCULO ES 5 o 6 EL INTERES EXTRA LOS SUMA AL SALDO INICIAL.
							V_IMP_SALDO_INICIAL = V_IMP_SALDO_INICIAL + 
				                        (DameInteresExtra(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, V_IMP_SALDO_INICIAL,
				                                          vlTasaIVA,pTxRespuesta,sql) * vlPlazo)
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
					//SE ELIMINA EL IVA DEL INTERES YA QUE ES UN CONCEPTO APARTE
					V_IMP_INTERES                  = V_IMP_SALDO_INICIAL * V_TASA_INTERES / (1+vlTasaIVA / 100)
					V_IMP_IVA_INTERES              = V_IMP_INTERES * (vlTasaIVA / 100)
					V_IMP_SALDO_FINAL              = V_IMP_SALDO_INICIAL - V_IMP_CAPITAL_AMORT
					V_FECHA_AMORTIZACION           = vlFReal
					vlFechaAmort                   = vlFReal



				}else{
					//EL PLAZO ES MAYOR A UNO
					V_IMP_SALDO_INICIAL     = V_IMP_SALDO_FINAL
					V_IMP_SALDO_FINAL       = V_IMP_SALDO_INICIAL - V_IMP_CAPITAL_AMORT

					if (vlCveMetodo != '01'){
					    //CUANDO LA CLAVE DEL METODO ES DIFERENTE DE UNO EL CALCULO DE INTERESES SE HACE SOBRE
					    //EL SALDO INSOLUTO
					    V_IMP_INTERES       = V_IMP_SALDO_INICIAL * V_TASA_INTERES / (1 + vlTasaIVA / 100)
					    V_IMP_IVA_INTERES   = V_IMP_INTERES * (vlTasaIVA / 100)
					}


				}
				// Se calcula la fecha de amortización
				vlFechaAmort  = vlFechaAmort + vlDiasPeriodicidad

				V_FECHA_AMORTIZACION = DameFechaValida(pCveGpoEmpresa, pCveEmpresa, vlFechaAmort, 'MX', pTxRespuesta,sql)

				// Cálculo del monto a pagar
				if (vlCveMetodo == '05' || vlCveMetodo == '06') {
					V.IMP_PAGO = (V.IMP_SALDO_INICIAL * V.TASA_INTERES) / 
							(1-(1 / (Math.pow((1 + V.TASA_INTERES),(vlPlazo - i + 1)))))
					V.IMP_CAPITAL_AMORT = V.IMP_PAGO - (V.IMP_INTERES + V.IMP_IVA_INTERES )
					V.IMP_SALDO_FINAL   = V.IMP_SALDO_INICIAL - V.IMP_CAPITAL_AMORT


				}else{
					//EL METODO DE CALCULO NO ES 5 NI 6
					//CALCULA EL IMPORTE TOTAL DEL PAGO
					V_IMP_PAGO = V_IMP_INTERES + V_IMP_IVA_INTERES + V_IMP_INTERES_EXTRA + 
				                     V_IMP_IVA_INTERES_EXTRA + V_IMP_CAPITAL_AMORT
				}

				//Inicializa variables
				V_B_PAGO_PUNTUAL                = cFalso
				V_IMP_INTERES_PAGADO            = 0
				V_IMP_INTERES_EXTRA_PAGADO      = 0
				V_IMP_CAPITAL_AMORT_PAGADO      = 0
				V_IMP_PAGO_PAGADO               = 0
				V_IMP_IVA_INTERES_PAGADO        = 0
				V_IMP_IVA_INTERES_EXTRA_PAGADO  = 0
				V_B_PAGADO                      = cFalso;
				V_FECHA_AMORT_PAGO_ULTIMO       
				V_IMP_PAGO_TARDIO               = 0
				V_IMP_PAGO_TARDIO_PAGADO        = 0
				V_IMP_INTERES_MORA              = 0
				V_IMP_INTERES_MORA_PAGADO       = 0
				V_IMP_IVA_INTERES_MORA          = 0
				V_IMP_IVA_INTERES_MORA_PAGADO   = 0
				V_F_VALOR_CALCULO               
				V_F_INI_AMORTIZACION            = V_FECHA_AMORTIZACION - vlDiasPeriodicidad;

				//Actualiza el importe de interes devengado por dia, no incluye el interes e IVA extra ya que solo 
				//considera el de la periodicidad estandar
				V_IMP_INTERES_DEV_X_DIA         = (V_IMP_INTERES + V_IMP_IVA_INTERES) / vlDiasPeriodicidad

				sql.execute("""				
					Insert into SIM_TABLA_AMORTIZACION(
						CVE_GPO_EMPRESA,
						CVE_EMPRESA,
						ID_PRESTAMO,
						NUM_PAGO_AMORTIZACION,
						FECHA_AMORTIZACION,
						IMP_SALDO_INICIAL,
						TASA_INTERES,
						IMP_INTERES,
						IMP_INTERES_EXTRA,
						IMP_CAPITAL_AMORT,
						IMP_PAGO,
						IMP_SALDO_FINAL,
						IMP_IVA_INTERES,
						IMP_IVA_INTERES_EXTRA,
						B_PAGO_PUNTUAL,
						IMP_INTERES_PAGADO,
						IMP_INTERES_EXTRA_PAGADO,
						IMP_CAPITAL_AMORT_PAGADO,
						IMP_PAGO_PAGADO,
						IMP_IVA_INTERES_PAGADO,
						IMP_IVA_INTERES_EXTRA_PAGADO,
						B_PAGADO,
						FECHA_AMORT_PAGO_ULTIMO,
						NUM_DIA_ATRASO,
						IMP_PAGO_TARDIO,
						IMP_PAGO_TARDIO_PAGADO,
						IMP_INTERES_MORA,
						IMP_INTERES_MORA_PAGADO,
						IMP_IVA_INTERES_MORA,
						IMP_IVA_INTERES_MORA_PAGADO,
						F_VALOR_CALCULO,
						IMP_INTERES_DEV_X_DIA,
						IMP_CAPITAL_AMORT_PREPAGO,
						F_INI_AMORTIZACION
						)values(
						${V_CVE_GPO_EMPRESA},
						${V_CVE_EMPRESA},
						${V_ID_PRESTAMO},
						${V_NUM_PAGO_AMORTIZACION},
						TO_DATE(${V_FECHA_AMORTIZACION},'DD-MM-YYYY'),
						${V_IMP_SALDO_FINAL},
						${V_TASA_INTERES},
						${V_IMP_INTERES},
						${V_IMP_INTERES_EXTRA},
						${V_IMP_CAPITAL_AMORT},
						${V_IMP_PAGO},
						${V_IMP_SALDO_INICIAL},
						${V_IMP_IVA_INTERES},
						${V_IMP_INTERES_EXTRA},
						${V_B_PAGO_PUNTUAL},
						${V_IMP_INTERES_PAGADO},
						${V_IMP_INTERES_EXTRA_PAGADO},
						${V_IMP_CAPITAL_AMORT_PAGADO},
						${V_IMP_PAGO_PAGADO},
						${V_IMP_IVA_INTERES_PAGADO},
						${V_IMP_IVA_INTERES_EXTRA_PAGADO},
						${V_B_PAGADO},
						${V_FECHA_AMORT_PAGO_ULTIMO},
						${V_NUM_DIA_ATRASO},
						${V_IMP_PAGO_TARDIO},
						${V_IMP_PAGO_TARDIO_PAGADO},
						${V_IMP_INTERES_MORA},
						${V_IMP_INTERES_MORA_PAGADO},
						${V_IMP_IVA_INTERES_MORA},
						${V_IMP_IVA_INTERES_MORA_PAGADO},
						TO_DATE(${V_F_VALOR_CALCULO},'DD-MM-YYYY'),
						${V_IMP_INTERES_DEV_X_DIA},
						${V_IMP_CAPITAL_AMORT_PREPAGO},
						TO_DATE(${V_F_INI_AMORTIZACION},'DD-MM-YYYY')
						)				
				""")
		
			}//END WHILE

			//ITERA TODOS LOS ACCESORIOS DEL PRESTAMO
			curAccesorios.each{ vlBufAccesorios ->
				// Se calcula el importe del accesorio
				
				sql.execute("""	
				INSERT INTO SIM_TABLA_AMORT_ACCESORIO(CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO, NUM_PAGO_AMORTIZACION, 
								ID_ACCESORIO, ID_FORMA_APLICACION, IMP_ACCESORIO, IMP_IVA_ACCESORIO, 
								IMP_ACCESORIO_PAGADO, IMP_IVA_ACCESORIO_PAGADO)
				SELECT CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO, NUM_PAGO_AMORTIZACION, 
					${vlBufAccesorios.ID_CARGO_COMISION}, ${vlBufAccesorios.ID_FORMA_APLICACION},
				       ROUND(
					    -- SI LA FORMA DE APLICACION ES IGUAL A 3 (Periódicamente dependiendo del monto prestado)
					    -- TOMA EL MONTO INICIAL, SI ES IGUAL A 5 (Periódicamente Cargo Fijo) TOMA EL VALOR DE UNO Y SI ES
					    -- IGUAL A 4 (Periódicamente dependiendo del saldo a la fecha de cálculo) TOMA EL SALDO INICIAL 
					    -- DE CADA SALDO INICIAL DE FECHA DE AMORTIZACION
					    DECODE(${vlBufAccesorios.ID_FORMA_APLICACION},3, ${vlMontoInicial},5,1,4,IMP_SALDO_INICIAL) *
					    -- EL RESULTADO SE MULTIPLICA POR EL VALOR DEL CARGO
					    ${vlBufAccesorios.VALOR_CARGO} /
					    -- EL RESULTADO ES DIVIDIDO ENTRE UNO SI LA FORMA DE APLICACION ES 5, EN CASO CONTRARIO ES DIVIDIDO
				 	    -- ENTRE EL VALOR DE LA UNIDAD
					    DECODE(${vlBufAccesorios.ID_FORMA_APLICACION},3,${vlBufAccesorios.VALOR_UNIDAD},5,1,4,
					    ${vlBufAccesorios.VALOR_UNIDAD}) /
					    -- DIVIDE ENTRE LA PERIODICIDAD DEL ACCESORIO Y EL RESULTADO SE MULTIPLICA POR LA PERIODICIDAD DEL 
					    -- PRODUCTO
					    ${vlBufAccesorios.DIAS_CARGO} * ${vlBufAccesorios.DIAS_PRODUCTO}
				       ,10) AS IMP_ACCESORIO, 
				       ROUND(
					    DECODE(${vlBufAccesorios.ID_FORMA_APLICACION},3, ${vlMontoInicial},5,1,4,IMP_SALDO_INICIAL) *
					    ${vlBufAccesorios.VALOR_CARGO} /
					    DECODE(${vlBufAccesorios.ID_FORMA_APLICACION},3,${vlBufAccesorios.VALOR_UNIDAD},5,1,4,
					    ${vlBufAccesorios.VALOR_UNIDAD}) /
					    ${vlBufAccesorios.DIAS_CARGO} * ${vlBufAccesorios.DIAS_PRODUCTO}
					    --CALCULA EL IVA DEL ACCESORIO, AL NO TENER IVA LOS ACCESORIOS EL IVA ES SIEMPRE 0
				       ,10) * (NVL(${vlBufAccesorios.TASA_IVA},1) - 1) AS IMP_IVA_ACCESORIO, 0, 0
				  FROM SIM_TABLA_AMORTIZACION
				 WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
				   AND CVE_EMPRESA     = ${pCveEmpresa}
				   AND ID_PRESTAMO     = ${pIdPrestamo}
				""")

			}//END CURSOR

			// Actualiza la informacion del credito
			def vlResultado = fActualizaInformacionCredito(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, vgFechaSistema, pTxRespuesta,sql)
		}//END IF

	}

	def DameTasaAmort(pCveGpoEmpresa,
                           pCveEmpresa,
                           pIdPrestamo,
                           pTasaIva,
                           pTxRespuesta,
			   sql){

		def vlTasaInteres
		//PARA EL CALCULO DE LA TASA SE VALIDA SI ESTA INDEXADA O NO INDEXADA A UN PAPEL,
		//EN CASO DE ESTAR INDEXADA EL VALOR DE LA TASA Y PERIODICIDAD LO TOMA DEL PAPEL DEFINIDO
		//SI NO ESTA INDEXADA TOMA LA TASA Y PERIODICIDAD DE LA TABLA PRESTAMO.

		//El INTERES DE LA TASA ES IGUAL A (VALOR TASA/ 100 / PERIODICIDAD DE LA TASA * PERIODICIDAD DEL PRODUCTO + IVA)
		//+ IVA ES IGUAL A MULTIPLICAR (1 + (IVA/100))
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
		//OBTIENE EL MONTO AUTORIZADO ASIGNADO AL PRESTAMO
		//SI NO EXISTE MONTO AUTORIZADO TOMA EL MONTO SOLICITADO
		//AL PARECER NO TIENE SENTIDO LA CONDICION DEL CLIENTE YA QUE UN PRESTAMO SOLO TIENE UN CLIENTE
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

		//SUMA LOS IMPORTES DE LOS CARGOS COMISIONES DONDE LA FORMA DE APLICACIONE ES IGUAL A CARGO INICIAL
		//SI NO EXISTE EL IMPORTE PARA EL CARGO COMISION TOMA UN PORCENTAJE DEL MONTO AUTORIZADO
		def rowMontoAutorizado = sql.firstRow(""" 
			SELECT SUM(NVL(C.CARGO_INICIAL, C.PORCENTAJE_MONTO / 100 * M.MONTO_AUTORIZADO) ) CARGO_INICIAL
			  FROM SIM_PRESTAMO_CARGO_COMISION C, SIM_CLIENTE_MONTO M
			 WHERE C.CVE_GPO_EMPRESA     = ${pCveGpoEmpresa}
			   AND C.CVE_EMPRESA         = ${pCveEmpresa}
			   AND C.ID_PRESTAMO         = ${pIdPrestamo}
			   AND C.ID_FORMA_APLICACION = 1 --CARGO INICIAL
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
		// OBTIENE EL VALOR DEL INTERES EXTRA MAS EL IVA
		// se calcula el importe de los intereses de los días adicionales y se divide entre el número de pagos

		def rowInteresExtra = sql.firstRow(""" 
			SELECT 	ROUND(${pSaldoInicial} * (NVL(P.FECHA_REAL, P.FECHA_ENTREGA) - P.FECHA_ENTREGA) / DECODE(P.TIPO_TASA, 'No indexada',
			  PT.DIAS, PTI.DIAS)*(DECODE(P.TIPO_TASA, 'No indexada', P.VALOR_TASA, T.VALOR) *
			  (1 + ${pTasaIva} / 100) ) /100 /P.PLAZO, 10)
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
		// SE OBTIENE LOS DATOS PARA VERIFICAR SI EL SIM OPERA SABADOS, DOMINGOS Y DIAS FESTIVOS
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

			//BUSCA SI LA FECHA EXISTE COMO UN DIA FESTIVO			
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
			//0 = Domingo, 7 = Sabado, en PL cambia los numeros.	
			vlDia = vlFechaTemp.getDay()    
       
			//BUSCA SI LA FECHA DE AMORTIZACION ES SABADO O DOMINGO O ES UN DIA FESTIVO
			if (vlDia == 0 || vlDia == 6 || esDiaFestivo == cVerdadero){
				if (vlBufParametro.B_OPERA_DOMINGO == 'V' && vlDia == '0'){
					//DIA ES IGUAL A DOMINGO Y PUEDE OPERAR LOS DOMINGOS
					vlFechaEncontrada = cVerdadero
				}else if(vlBufParametro.B_OPERA_SABADO =='V' && vlDia == '6'){
					//DIA ES IGUAL A SABADO Y PUEDE OPERAR LOS SABADOS
					vlFechaEncontrada = cVerdadero
				}else if(vlBufParametro.B_OPERA_DIA_FESTIVO =='V' &&  esDiaFestivo == cVerdadero){
					//LA FECHA ES UN DIA FESTIVO Y PUEDE OPERAR LOS DIAS FESTIVOS
					vlFechaEncontrada = cVerdadero
				}else{
					//LA FECHA ES DIA FESTIVO O SABADO O DOMINGO SIN EMBARGO EL SISTEMA NO OPERA EN ESTOS DIAS
					//LA FECHA SE LE SUMA UN DIA
					vlFechaTemp = vlFechaTemp + 1
				}
			}else{
				vlFechaEncontrada = cVerdadero
			}

		}// END WHILE
		return	vlFechaTempFormato	

	}

	def fActualizaInformacionCredito(pCveGpoEmpresa,
		                          pCveEmpresa,
		                          pIdPrestamo,
		                          pFValor,
		                          pTxRespuesta,
					  sql){

		def vlBEsGrupo
		def vlIdGrupo
		def vlInteresMora
		def vlIVAInteresMora
		def vlImpDeudaMinima
		
		def vgResultado
		def vlCase = 0


		// Determina si el prestamo es un prestamo grupal

		if (pIdPrestamo != 0){
			def vlBufPrestGrupal = sql.firstRow(""" 
			  SELECT ${cVerdadero}VERDADERO, ID_GRUPO
			    FROM SIM_PRESTAMO_GRUPO
			   WHERE CVE_GPO_EMPRESA   = ${pCveGpoEmpresa}
			     AND CVE_EMPRESA       = ${pCveEmpresa}
			     AND ID_PRESTAMO_GRUPO = ${pIdPrestamo}
			""")
			vlBEsGrupo = vlBufPrestGrupal.VERDADERO
			vlIdGrupo =  vlBufPrestGrupal.ID_GRUPO
			vlCase = cVerdadero
		}

		// Recupera el importe de la deuda minima

		def vlBufDeudaMinima = sql.firstRow(""" 
			SELECT IMP_DEUDA_MINIMA
			 FROM SIM_PARAMETRO_GLOBAL 
			WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
			      CVE_EMPRESA     = ${pCveEmpresa}    
		""")

		vlImpDeudaMinima  = vlBufDeudaMinima.IMP_DEUDA_MINIMA

		//SI SE GENERA UN PRESTAMO INDIVIDUAL CON ID 1 Y POSTERIORMENTE SE GENERA UN CREDITO GRUPAL CON ID 1,
		//CON ESTA VALIDANCION NO SE PUEDO ACTUALIZAR LA INFORMACION DEL PRESTAMO INDIVIDUAL 1
		switch (vlCase) {

			 case 0:
				// RECUPERA LAS AMORTIZACIONES DE LOS PRESTAMOS QUE NO HAN SIDO CUBIERTOS, MENORES A LA FECHA VALOR INDICADA
				// Y QUE EL TIPO DE RECARGO ES MONTO FIJO O (MONTO FIJO E INTERES MORATORIO). 
				// EL MONTO FIJO POR PERIODO DEBE SER MAYOR A 0
				// Recupera las tablas de amortizacion de todos los prestamos
				def curTodo = []
				sql.eachRow("""	
					   SELECT T.CVE_GPO_EMPRESA, T.CVE_EMPRESA, T.ID_PRESTAMO, T.NUM_PAGO_AMORTIZACION, 
						  ROUND(NVL(P.MONTO_FIJO_PERIODO,0),2) AS IMP_PAGO_TARDIO
					     FROM SIM_TABLA_AMORTIZACION T, SIM_PRESTAMO P
					    WHERE T.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
					      AND T.CVE_EMPRESA           = ${pCveEmpresa}
					      AND T.FECHA_AMORTIZACION    < ${pFValor}
					      AND T.B_PAGADO              = ${cFalso}
					      AND T.CVE_GPO_EMPRESA       = P.CVE_GPO_EMPRESA
					      AND T.CVE_EMPRESA           = P.CVE_EMPRESA
					      AND T.ID_PRESTAMO           = P.ID_PRESTAMO
					      AND P.ID_TIPO_RECARGO    IN (4,5)
					      AND NVL(P.MONTO_FIJO_PERIODO,0) > 0
					      ORDER BY T.ID_PRESTAMO, T.NUM_PAGO_AMORTIZACION -- CODIGO AÑADIDO AL ORIGINAL
				""") {
				  curTodo << it.toRowResult()
				}
				//Procesa todos los creditos
				//AL PARECER EN EL SISTEMA NO ESTA CONTEMPLADO CUANDO EL TIPO DE RECARGO ES IGUAL A 3
				//ES DECIR CUANDO EL TIPO DE RECARGO CONTEMPLA SOLO LOS INTERESES MORATORIOS
				curTodo.each{ vlBufAmorizacion ->
					//EXPLICARME COMO FUNCIONA LOS INTERESES MORATORIOS **********************
					def interesMora = pDameInteresMoratorio(vlBufAmorizacion.CVE_GPO_EMPRESA, vlBufAmorizacion.CVE_EMPRESA,
							 vlBufAmorizacion.ID_PRESTAMO,vlBufAmorizacion.NUM_PAGO_AMORTIZACION, pFValor,
							 vlInteresMora, vlIVAInteresMora, pTxRespuesta,sql)

					vlInteresMora = interesMora.pInteresMora
					vlIVAInteresMora = interesMora.pIVAInteresMora

					
					//FORMATO DE LAS FECHA AMORTIZACION
					SimpleDateFormat sdf = new SimpleDateFormat();
					sdf = new SimpleDateFormat("dd-MM-yyyy");
					def pFValorFormato = sdf.format(pFValor)
				
					//ASIGNA A CADA AMORTIZACION LOS RECARGOS Y EL NUMERO DE DIAS DE ATRASO DEL PAGO
					sql.executeUpdate """
						 UPDATE SIM_TABLA_AMORTIZACION
						      SET IMP_PAGO_TARDIO      = ${vlBufAmorizacion.IMP_PAGO_TARDIO},
							  IMP_INTERES_MORA     = ${vlInteresMora},
							  IMP_IVA_INTERES_MORA = ${vlIVAInteresMora},
							  F_VALOR_CALCULO      = TO_DATE(${pFValorFormato},'DD-MM-YYYY'),
							  NUM_DIA_ATRASO       = CASE 
								                    WHEN B_PAGADO = ${cFalso} AND (IMP_CAPITAL_AMORT -
											 IMP_CAPITAL_AMORT_PAGADO) >= ${vlImpDeudaMinima} THEN 
											 TO_DATE(${pFValorFormato},'DD-MM-YYYY')-FECHA_AMORTIZACION
								-- SI YA ESTA PAGADO O LA DEUDA ES MENOR A LA DEUDA MINIMA LOS NUMERO DE DIAS DE 
								-- ATRASO SON IGUALES A LA FECHA DE ULTIMO PAGO MENOS LA FECHA DE AMORTIZACION 
								                    WHEN (B_PAGADO = ${cVerdadero} OR (IMP_CAPITAL_AMORT -
											 IMP_CAPITAL_AMORT_PAGADO) < ${vlImpDeudaMinima}) AND
											 NVL(FECHA_AMORT_PAGO_ULTIMO, FECHA_AMORTIZACION) >
											 FECHA_AMORTIZACION THEN 
											 FECHA_AMORT_PAGO_ULTIMO - FECHA_AMORTIZACION
								                    ELSE 0
								                 END
						    WHERE CVE_GPO_EMPRESA       = ${vlBufAmorizacion.CVE_GPO_EMPRESA}
						      AND CVE_EMPRESA           = ${vlBufAmorizacion.CVE_EMPRESA}
						      AND ID_PRESTAMO           = ${vlBufAmorizacion.ID_PRESTAMO}
						      AND NUM_PAGO_AMORTIZACION = ${vlBufAmorizacion.NUM_PAGO_AMORTIZACION}
					"""

				}// END curTodo

				// Recupera la informacion de dias de atraso de todos los creditos
				// EL CURSOR RECUPERA LA INFORMACION DE DIAS DE ATRASO DE TODOS LOS CREDITOS
				def curDiasAtrasoTodo = []
				sql.eachRow("""	
					   SELECT CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO, MAX(NUM_DIA_ATRASO) AS NUM_DIA_ATRASO
					     FROM SIM_TABLA_AMORTIZACION 
					    WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
					      AND CVE_EMPRESA           = ${pCveEmpresa}
					      AND NUM_DIA_ATRASO        > 0
					   GROUP BY CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO
				"""){
				  curDiasAtrasoTodo << it.toRowResult()
				}


				// Actualiza la informacion del atraso (SIM_PRESTAMO) para los prestamos individuales
				// ACTUALIZA LOS DATOS NUM
				curDiasAtrasoTodo.each{ vlBufPrestamo ->
					sql.executeUpdate NUM_DIAS_ATRASO_ACTUAL Y NUM_DIAS_ATRASO_MAX
						UPDATE SIM_PRESTAMO
						SET NUM_DIAS_ATRASO_ACTUAL = ${vlBufPrestamo.NUM_DIA_ATRASO},
						  NUM_DIAS_ATRASO_MAX    = CASE WHEN ${vlBufPrestamo.NUM_DIA_ATRASO} > NVL(NUM_DIAS_ATRASO_MAX,0) 
										THEN ${vlBufPrestamo.NUM_DIA_ATRASO}
										ELSE NUM_DIAS_ATRASO_MAX
									   END
						WHERE CVE_GPO_EMPRESA = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA     = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO     = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}//END curDiasAtrasoTodo

				// Recupera la informacion de dias de atraso de los prestamos de todos los creditos grupales
				// CADA ID PRESTAMO EXISTE EN LA TABLA SIM_PRESTAMO_GPO_DET COMO UN CREDITO INDIVIDUAL DE UN GRUPO
				// SIEMPRE VA ASIGNAR EL NUMERO DE DIAS DEL INTEGRANTE QUE TIENE MAYOR DIAS DE ATRASO
				def curDiasAtrasoPresGpoTodo = []
				sql.eachRow("""	
					   SELECT G.CVE_GPO_EMPRESA, G.CVE_EMPRESA, G.ID_PRESTAMO_GRUPO AS ID_PRESTAMO, 
						MAX(P.NUM_DIAS_ATRASO_ACTUAL) AS NUM_DIA_ATRASO
					     FROM SIM_PRESTAMO_GPO_DET G, SIM_PRESTAMO P
					    WHERE G.CVE_GPO_EMPRESA        = ${pCveGpoEmpresa}
					      AND G.CVE_EMPRESA            = ${pCveEmpresa}
					      AND G.CVE_GPO_EMPRESA        = P.CVE_GPO_EMPRESA
					      AND G.CVE_EMPRESA            = P.CVE_EMPRESA
					      AND G.ID_PRESTAMO            = P.ID_PRESTAMO
					      AND P.NUM_DIAS_ATRASO_ACTUAL > 0
					    GROUP BY G.CVE_GPO_EMPRESA, G.CVE_EMPRESA, G.ID_PRESTAMO_GRUPO
				"""){
				  curDiasAtrasoPresGpoTodo << it.toRowResult()
				}

				// Actualiza la informacion del atraso (SIM_PRESTAMO_GPO) para los prestamos grupales
				curDiasAtrasoPresGpoTodo.each{ vlBufPrestamo ->
					sql.executeUpdate """
					   UPDATE SIM_PRESTAMO_GRUPO
					      SET NUM_DIAS_ATRASO_ACTUAL = ${vlBufPrestamo.NUM_DIA_ATRASO},
						  NUM_DIAS_ATRASO_MAX    = CASE WHEN ${vlBufPrestamo.NUM_DIA_ATRASO} > NVL(NUM_DIAS_ATRASO_MAX,0) 
										THEN ${vlBufPrestamo.NUM_DIA_ATRASO}
						                                ELSE NUM_DIAS_ATRASO_MAX
						                           END
					    WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO_GRUPO = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}//END curDiasAtrasoPresGpoTodo

				// Recupera la informacion de la categoria de todos los prestamos individuales
				def curPrestamoIndCatTodo = []
				sql.eachRow("""	
					   SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.CVE_CATEGORIA_ATRASO
					     FROM SIM_PRESTAMO A, SIM_CATEGORIA_ATRASO B
					    WHERE A.CVE_GPO_EMPRESA            = ${pCveGpoEmpresa} AND
						  A.Cve_Empresa                = ${pCveEmpresa}    And
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) >= 0                AND
						  A.CVE_GPO_EMPRESA            = B.CVE_GPO_EMPRESA AND
						  A.CVE_EMPRESA                = B.CVE_EMPRESA     AND
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) BETWEEN B.NUM_DIAS_ATRASO_MIN AND B.NUM_DIAS_ATRASO_MAX AND
						  B.SIT_CATEGORIA_ATRASO = 'AC'
				"""){
				  curPrestamoIndCatTodo << it.toRowResult()
				}	

				// Actualiza la categoria de todos los prestamos individuales
				curPrestamoIndCatTodo.each{ vlBufPrestamo ->
					sql.executeUpdate """
					   UPDATE SIM_PRESTAMO
					      SET CVE_CATEGORIA_ATRASO = ${vlBufPrestamo.CVE_CATEGORIA_ATRASO}
					    WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO       = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}//END curPrestamoIndCatTodo

				// Recupera la informacion de la categoria de todos los prestamos grupales
				def curPrestamoGpoCatTodo = []
				sql.eachRow("""	
					   SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO AS ID_PRESTAMO, B.CVE_CATEGORIA_ATRASO
					     FROM SIM_PRESTAMO_GRUPO A, SIM_CATEGORIA_ATRASO B
					    WHERE A.CVE_GPO_EMPRESA            = ${pCveGpoEmpresa}    AND
						  A.Cve_Empresa                = ${pCveEmpresa}       And
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) >= 0                 AND
						  A.CVE_GPO_EMPRESA            = B.CVE_GPO_EMPRESA AND
						  A.CVE_EMPRESA                = B.CVE_EMPRESA     AND
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) BETWEEN B.NUM_DIAS_ATRASO_MIN AND B.NUM_DIAS_ATRASO_MAX AND
						  B.SIT_CATEGORIA_ATRASO =     'AC'
				"""){
				  curPrestamoGpoCatTodo << it.toRowResult()
				}	

				// Actualiza la categoria de todos los prestamos grupales
				curPrestamoGpoCatTodo.each{ vlBufPrestamo ->
					sql.executeUpdate """
					   UPDATE SIM_PRESTAMO_GRUPO
					      SET CVE_CATEGORIA_ATRASO = ${vlBufPrestamo.CVE_CATEGORIA_ATRASO}
					    WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO_GRUPO = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}//END curPrestamoGpoCatTodo

				//Actualiza la categoria F para todos los prestamos individuales
				//F SIGNIFICA "con ningún pago dentro de los 40 días anteriores son F"
				//PUEDE DARSE EL CASO QUE NUM_DIAS_ATRASO_ACTUAL > 39 Y HAYA HECHO UN PAGO HACE UN DIA QUE NO CUBRIO NINGUNA
				//AMORTIZACION, EL SISTEMA DEBE ASIGNAR CATEGORIA E YA QUE AL MENOS HIZO UN PAGO EN LOS ULTIMOS 40 DIAS
				sql.executeUpdate """
				       UPDATE SIM_PRESTAMO
					  SET CVE_CATEGORIA_ATRASO = 'F'
					WHERE CVE_GPO_EMPRESA               = ${pCveGpoEmpresa} AND
					      CVE_EMPRESA                   = ${pCveEmpresa}    AND
					      NVL(CVE_CATEGORIA_ATRASO,'X') = 'E'               AND
					      NVL(NUM_DIAS_ATRASO_ACTUAL,0) > 39
				"""

				//Actualiza el numero de integrantes de todos los grupos       
				sql.executeUpdate """
				       UPDATE SIM_GRUPO A
					  SET NUM_INTEGRANTES = (SELECT COUNT(1) 
						                   FROM SIM_GRUPO_INTEGRANTE
						                  WHERE CVE_GPO_EMPRESA = A.CVE_GPO_EMPRESA AND
						                        CVE_EMPRESA     = A.CVE_EMPRESA     AND
						                        ID_GRUPO        = A.ID_GRUPO        AND
						                        FECHA_BAJA_LOGICA IS NULL)
				       WHERE A.CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
					     A.CVE_EMPRESA     = ${pCveEmpresa}
				"""
			       // Actualiza los dias de antiguedad de todos los prestamos
			       vgResultado = actualizaDiasAntiguedad (pCveGpoEmpresa, pCveEmpresa, pFValor, vlImpDeudaMinima,sql);

				// Actualiza las fechas de todos los prestamos
				// Actualiza las fechas de todos los prestamos individuales
				vgResultado = actualizaFechasPrestamo (pCveGpoEmpresa, pCveEmpresa, 0, vlBEsGrupo,sql);

				//Actualiza las fechas de todos los prestamos grupales
				vgResultado = actualizaFechasPrestamoGpo (pCveGpoEmpresa, pCveEmpresa, 0, sql)
				break
			 case cVerdadero:
				// RECUPERA LAS AMORTIZACIONES DE LOS PRESTAMOS QUE NO HAN SIDO CUBIERTOS, MENORES A LA FECHA VALOR INDICADA
				// Y QUE EL TIPO DE RECARGO ES MONTO FIJO O (MONTO FIJO E INTERES MORATORIO). 
				// EL MONTO FIJO POR PERIODO DEBE SER MAYOR A 0

				//Recupera las tablas de amortizacion de un prestamo grupal
				def curPorGpoPrestamo = []
				sql.eachRow("""
					   SELECT T.CVE_GPO_EMPRESA, T.CVE_EMPRESA, T.ID_PRESTAMO, T.NUM_PAGO_AMORTIZACION, 
						  ROUND(NVL(P.MONTO_FIJO_PERIODO,0),2) AS IMP_PAGO_TARDIO
					     FROM SIM_PRESTAMO_GPO_DET G, SIM_TABLA_AMORTIZACION T, SIM_PRESTAMO P
					    WHERE G.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
					      AND G.CVE_EMPRESA           = ${pCveEmpresa}
					      AND G.ID_PRESTAMO_GRUPO     = ${pIdPrestamo}
					      AND G.CVE_GPO_EMPRESA       = T.CVE_GPO_EMPRESA
					      AND G.CVE_EMPRESA           = T.CVE_EMPRESA
					      AND G.ID_PRESTAMO           = T.ID_PRESTAMO
					      AND T.FECHA_AMORTIZACION    < ${pFValor}
					      AND T.B_PAGADO              = ${cFalso}
					      AND T.CVE_GPO_EMPRESA       = P.CVE_GPO_EMPRESA
					      AND T.CVE_EMPRESA           = P.CVE_EMPRESA
					      AND T.ID_PRESTAMO           = P.ID_PRESTAMO
					      AND P.ID_TIPO_RECARGO    IN (4,5)
					      AND NVL(P.MONTO_FIJO_PERIODO,0) > 0
				""") {
				  curPorGpoPrestamo << it.toRowResult()
				}

				//Procesa un grupo
				curPorGpoPrestamo.each{ vlBufAmorizacion ->
					def interesMora = pDameInteresMoratorio(vlBufAmorizacion.CVE_GPO_EMPRESA, vlBufAmorizacion.CVE_EMPRESA,
							 vlBufAmorizacion.ID_PRESTAMO,vlBufAmorizacion.NUM_PAGO_AMORTIZACION, pFValor,
							 vlInteresMora, vlIVAInteresMora, pTxRespuesta,sql)

					vlInteresMora = interesMora.pInteresMora
					vlIVAInteresMora = interesMora.pIVAInteresMora

					//FORMATO DE LAS FECHA AMORTIZACION
					SimpleDateFormat sdf = new SimpleDateFormat();
					sdf = new SimpleDateFormat("dd-MM-yyyy");
					def pFValorFormato = sdf.format(pFValor)

					sql.executeUpdate """
						  UPDATE SIM_TABLA_AMORTIZACION
						      SET IMP_PAGO_TARDIO      = ${vlBufAmorizacion.IMP_PAGO_TARDIO},
							  IMP_INTERES_MORA     = ${vlInteresMora},
							  IMP_IVA_INTERES_MORA = ${vlIVAInteresMora},
							  F_VALOR_CALCULO      = TO_DATE(${pFValorFormato},'DD-MM-YYYY'),
							  NUM_DIA_ATRASO       = CASE 
								                    WHEN B_PAGADO = ${cFalso} AND (IMP_CAPITAL_AMORT -
										     IMP_CAPITAL_AMORT_PAGADO) >= ${vlImpDeudaMinima} 
										     THEN TO_DATE(${pFValorFormato},'DD-MM-YYYY') -
										     FECHA_AMORTIZACION
								                    WHEN (B_PAGADO = ${cVerdadero} OR (IMP_CAPITAL_AMORT -
										     IMP_CAPITAL_AMORT_PAGADO) < ${vlImpDeudaMinima}) AND 
								                     NVL(FECHA_AMORT_PAGO_ULTIMO, FECHA_AMORTIZACION) >
										     FECHA_AMORTIZACION THEN FECHA_AMORT_PAGO_ULTIMO -
										     FECHA_AMORTIZACION
								                    ELSE 0
								                 END
						    WHERE CVE_GPO_EMPRESA       = ${vlBufAmorizacion.CVE_GPO_EMPRESA}
						      AND CVE_EMPRESA           = ${vlBufAmorizacion.CVE_EMPRESA}
						      AND ID_PRESTAMO           = ${vlBufAmorizacion.ID_PRESTAMO}
						      AND NUM_PAGO_AMORTIZACION = ${vlBufAmorizacion.NUM_PAGO_AMORTIZACION}

					"""
				}//END curPorGpoPrestamo

				// Recupera la informacion de dias de atraso de las tablas de amortizacion de un credito grupal
				def curDiasAtrasoGpo = []
				sql.eachRow("""	
					   SELECT T.CVE_GPO_EMPRESA, T.CVE_EMPRESA, T.ID_PRESTAMO, MAX(NUM_DIA_ATRASO) AS NUM_DIA_ATRASO
					     FROM SIM_PRESTAMO_GPO_DET G, SIM_TABLA_AMORTIZACION T
					    WHERE G.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
					      AND G.CVE_EMPRESA           = ${pCveEmpresa}
					      AND G.ID_PRESTAMO_GRUPO     = ${pIdPrestamo}
					      AND G.CVE_GPO_EMPRESA       = T.CVE_GPO_EMPRESA
					      AND G.CVE_EMPRESA           = T.CVE_EMPRESA
					      AND G.ID_PRESTAMO           = T.ID_PRESTAMO
					      AND T.NUM_DIA_ATRASO        > 0
					 GROUP BY T.CVE_GPO_EMPRESA, T.CVE_EMPRESA, T.ID_PRESTAMO
				"""){
				  curDiasAtrasoGpo << it.toRowResult()
				}
			    
				// Actualiza la informacion del atraso (SIM_PRESTAMO) para los prestamos grupales
				curDiasAtrasoGpo.each{ vlBufPrestamo ->
					sql.executeUpdate """
						   UPDATE SIM_PRESTAMO
						      SET NUM_DIAS_ATRASO_ACTUAL = ${vlBufPrestamo.NUM_DIA_ATRASO},
							  NUM_DIAS_ATRASO_MAX    = CASE WHEN ${vlBufPrestamo.NUM_DIA_ATRASO} > 
											NVL(NUM_DIAS_ATRASO_MAX,0) 
											THEN ${vlBufPrestamo.NUM_DIA_ATRASO}
								                        ELSE NUM_DIAS_ATRASO_MAX
								                   END
						    WHERE CVE_GPO_EMPRESA = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
							  CVE_EMPRESA     = ${vlBufPrestamo.CVE_EMPRESA}     AND
							  ID_PRESTAMO     = ${vlBufPrestamo.ID_PRESTAMO}

					"""
				}

				//Recupera la informacion de dias de atraso de los prestamos de todos los creditos grupales
				def curDiasAtrasoPresGpoXPres = []
				sql.eachRow("""	
					   SELECT G.CVE_GPO_EMPRESA, G.CVE_EMPRESA, G.ID_PRESTAMO_GRUPO AS ID_PRESTAMO, 
						MAX(P.NUM_DIAS_ATRASO_ACTUAL) AS NUM_DIA_ATRASO
					     FROM SIM_PRESTAMO_GPO_DET G, SIM_PRESTAMO P
					    WHERE G.CVE_GPO_EMPRESA        = ${pCveGpoEmpresa}
					      AND G.CVE_EMPRESA            = ${pCveEmpresa}
					      AND G.ID_PRESTAMO_GRUPO      = ${pIdPrestamo}
					      AND G.CVE_GPO_EMPRESA        = P.CVE_GPO_EMPRESA
					      AND G.CVE_EMPRESA            = P.CVE_EMPRESA
					      AND G.ID_PRESTAMO            = P.ID_PRESTAMO
					      AND P.NUM_DIAS_ATRASO_ACTUAL > 0
					    GROUP BY G.CVE_GPO_EMPRESA, G.CVE_EMPRESA, G.ID_PRESTAMO_GRUPO
				"""){
				  curDiasAtrasoPresGpoXPres << it.toRowResult()
				}
			
				// Actualiza la informacion del atraso (SIM_PRESTAMO_GPO) para los prestamos grupales
				curDiasAtrasoPresGpoXPres.each{ vlBufPrestamo ->
					sql.executeUpdate """
                  			UPDATE SIM_PRESTAMO_GRUPO
					      SET NUM_DIAS_ATRASO_ACTUAL = ${vlBufPrestamo.NUM_DIA_ATRASO},
						  NUM_DIAS_ATRASO_MAX    = CASE WHEN ${vlBufPrestamo.NUM_DIA_ATRASO} > NVL(NUM_DIAS_ATRASO_MAX,0) 
										THEN ${vlBufPrestamo.NUM_DIA_ATRASO}
							                        ELSE NUM_DIAS_ATRASO_MAX
							                   END
					    WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO_GRUPO = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}

				//Recupera la informacion de la categoria de un prestamos grupal
				def curPrestamoGpoCatXPres = []
				sql.eachRow("""	  
					   SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO AS ID_PRESTAMO, B.CVE_CATEGORIA_ATRASO
					     FROM SIM_PRESTAMO_GRUPO A, SIM_CATEGORIA_ATRASO B
					    WHERE A.CVE_GPO_EMPRESA            = ${pCveGpoEmpresa}    AND
						  A.CVE_EMPRESA                = ${pCveEmpresa}       AND
						  A.Id_Prestamo_Grupo          = ${pIdPrestamo}       And
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) >= 0                 AND
						  A.CVE_GPO_EMPRESA            = B.CVE_GPO_EMPRESA AND
						  A.CVE_EMPRESA                = B.CVE_EMPRESA     AND
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) BETWEEN B.NUM_DIAS_ATRASO_MIN AND B.NUM_DIAS_ATRASO_MAX AND
						  B.SIT_CATEGORIA_ATRASO       = 'AC'
				"""){
				  curPrestamoGpoCatXPres << it.toRowResult()
				}

				// Actualiza la categoria de un prestamo grupal
				curPrestamoGpoCatXPres.each{ vlBufPrestamo ->
					sql.executeUpdate """
					  UPDATE SIM_PRESTAMO_GRUPO
					  	SET CVE_CATEGORIA_ATRASO = ${vlBufPrestamo.CVE_CATEGORIA_ATRASO}
					  	WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO_GRUPO = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}

				// Actualiza la categoria F para el prestamo grupal
				sql.executeUpdate """
				       UPDATE SIM_PRESTAMO_GRUPO
					  SET CVE_CATEGORIA_ATRASO = 'F'
					WHERE CVE_GPO_EMPRESA               = ${pCveGpoEmpresa} AND
					      CVE_EMPRESA                   = ${pCveEmpresa}    AND
					      ID_PRESTAMO_GRUPO             = ${pIdPrestamo}    AND
					      NVL(CVE_CATEGORIA_ATRASO,'X') = 'E'            AND
					      NVL(NUM_DIAS_ATRASO_ACTUAL,0) > 39
				"""
				// Actualiza los integrantes del grupo                       
				sql.executeUpdate """
				       UPDATE SIM_GRUPO A
					  SET NUM_INTEGRANTES = (SELECT COUNT(1) 
						                   FROM SIM_GRUPO_INTEGRANTE
						                  WHERE CVE_GPO_EMPRESA = A.CVE_GPO_EMPRESA AND
						                        CVE_EMPRESA     = A.CVE_EMPRESA     AND
						                        ID_GRUPO        = A.ID_GRUPO        AND
						                        FECHA_BAJA_LOGICA IS NULL)
				       WHERE A.CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
					     A.CVE_EMPRESA     = ${pCveEmpresa}    AND
					     A.ID_GRUPO        = ${vlIdGrupo}
				"""

				// Actualiza las fecha del prestamo grupal
				// Actualiza las fechas de todos los prestamos individuales del grupo
				vgResultado = actualizaFechasPrestamo (pCveGpoEmpresa, pCveEmpresa, 0, vlBEsGrupo, sql);               
				// Actualiza las fechas del prestamo grupal
				vgResultado = actualizaFechasPrestamoGpo (pCveGpoEmpresa, pCveEmpresa, vlIdGrupo, sql);

				break
			 default:
				println "Procesa un credito individual"
				//Procesa un credito individual

				//Recupera las tablas de amortizacion de un prestamo
				//OBTIENE LAS AMORTIZACIONES MENORES A LA FECHA VALOR PASADA COMO PARAMETRO Y QUE NO HAYAN SIDO PAGADAS 
				//DE UN PRESTAMO
				def curPorPrestamo = []
				sql.eachRow("""
					  SELECT T.CVE_GPO_EMPRESA, T.CVE_EMPRESA, T.ID_PRESTAMO, T.NUM_PAGO_AMORTIZACION, 
						  ROUND(NVL(P.MONTO_FIJO_PERIODO,0),2) AS IMP_PAGO_TARDIO
					     FROM SIM_TABLA_AMORTIZACION T, SIM_PRESTAMO P
					    WHERE T.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
					      AND T.CVE_EMPRESA           = ${pCveEmpresa}
					      AND T.ID_PRESTAMO           = ${pIdPrestamo}
					      AND T.FECHA_AMORTIZACION    < ${pFValor}
					      AND T.B_PAGADO              = ${cFalso}
					      AND T.CVE_GPO_EMPRESA       = P.CVE_GPO_EMPRESA
					      AND T.CVE_EMPRESA           = P.CVE_EMPRESA
					      AND T.ID_PRESTAMO           = P.ID_PRESTAMO
					      --TIPO DE RECARGO LO OBTIENE DE LA TABLA SIM_CAT_ACCESORIO DONDE LA CLAVE DEL ACCESORIO ES IGUAL A
					      --RECARGO, TIPO DE RECARGO 4 (MONTO FIJO POR PERIODO), 5 (INTERES MORATORIO Y MONTO FIJO POR PERIODO)
					      AND P.ID_TIPO_RECARGO    IN (4,5)
					      AND NVL(P.MONTO_FIJO_PERIODO,0) > 0
				""") {
				  curPorPrestamo << it.toRowResult()
				}

				curPorPrestamo.each{ vlBufAmorizacion ->
					
					def interesMora = pDameInteresMoratorio(vlBufAmorizacion.CVE_GPO_EMPRESA, vlBufAmorizacion.CVE_EMPRESA,
							 vlBufAmorizacion.ID_PRESTAMO,vlBufAmorizacion.NUM_PAGO_AMORTIZACION, pFValor,
							 vlInteresMora, vlIVAInteresMora, pTxRespuesta,sql)

					vlInteresMora = interesMora.pInteresMora
					vlIVAInteresMora = interesMora.pIVAInteresMora

					//FORMATO DE LAS FECHA AMORTIZACION
					SimpleDateFormat sdf = new SimpleDateFormat();
					sdf = new SimpleDateFormat("dd-MM-yyyy");
					def pFValorFormato = sdf.format(pFValor)

					sql.executeUpdate """
					  UPDATE SIM_TABLA_AMORTIZACION
					      SET IMP_PAGO_TARDIO      = ${vlBufAmorizacion.IMP_PAGO_TARDIO},
						  IMP_INTERES_MORA     = ${vlInteresMora},
						  IMP_IVA_INTERES_MORA = ${vlIVAInteresMora},
						  F_VALOR_CALCULO      = TO_DATE(${pFValorFormato},'DD-MM-YYYY'),
						  NUM_DIA_ATRASO       = CASE 
						                            WHEN B_PAGADO = ${cFalso} AND (IMP_CAPITAL_AMORT -
									     IMP_CAPITAL_AMORT_PAGADO) >= ${vlImpDeudaMinima} 
									     THEN TO_DATE(${pFValorFormato},'DD-MM-YYYY') - FECHA_AMORTIZACION
						                            WHEN (B_PAGADO = ${cVerdadero} OR (IMP_CAPITAL_AMORT -
									     IMP_CAPITAL_AMORT_PAGADO) < ${vlImpDeudaMinima}) AND
	    				                                     NVL(FECHA_AMORT_PAGO_ULTIMO, FECHA_AMORTIZACION) > 
									     FECHA_AMORTIZACION THEN FECHA_AMORT_PAGO_ULTIMO - 
									     FECHA_AMORTIZACION
						                            ELSE 0
						                         END
					    WHERE CVE_GPO_EMPRESA       = ${vlBufAmorizacion.CVE_GPO_EMPRESA}
					      AND CVE_EMPRESA           = ${vlBufAmorizacion.CVE_EMPRESA}
					      AND ID_PRESTAMO           = ${vlBufAmorizacion.ID_PRESTAMO}
					      AND NUM_PAGO_AMORTIZACION = ${vlBufAmorizacion.NUM_PAGO_AMORTIZACION}

					"""
				}//END curPorPrestamo

				// Recupera la informacion de dias de atraso de un credito individual
				def curDiasAtrasoPres = []
				sql.eachRow("""	
					   SELECT CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO, MAX(NUM_DIA_ATRASO) AS NUM_DIA_ATRASO
					     FROM SIM_TABLA_AMORTIZACION 
					    WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
					      AND CVE_EMPRESA           = ${pCveEmpresa}
					      AND ID_PRESTAMO           = ${pIdPrestamo}
					      AND NUM_DIA_ATRASO        > 0
					    GROUP BY CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO
				"""){
				  curDiasAtrasoPres << it.toRowResult()
				}

				// Actualiza la informacion del atraso (SIM_PRESTAMO) para los prestamos individuales
				curDiasAtrasoPres.each{ vlBufPrestamo ->
					sql.executeUpdate """
					  UPDATE SIM_PRESTAMO
					      SET NUM_DIAS_ATRASO_ACTUAL = ${vlBufPrestamo.NUM_DIA_ATRASO},
						  NUM_DIAS_ATRASO_MAX    = CASE WHEN ${vlBufPrestamo.NUM_DIA_ATRASO} > NVL(NUM_DIAS_ATRASO_MAX,0) 
						  				THEN ${vlBufPrestamo.NUM_DIA_ATRASO}
						                                ELSE NUM_DIAS_ATRASO_MAX
						                           END
					    WHERE CVE_GPO_EMPRESA = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA     = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO     = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}
    
				// Recupera la informacion de la categoria de un prestamo individual
				def curPrestamoIndCatXPres = []
				sql.eachRow("""	
					   SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.CVE_CATEGORIA_ATRASO
					     FROM SIM_PRESTAMO A, SIM_CATEGORIA_ATRASO B
					    WHERE A.CVE_GPO_EMPRESA            = ${pCveGpoEmpresa}    AND
						  A.CVE_EMPRESA                = ${pCveEmpresa}       AND
						  A.Id_Prestamo                = ${pIdPrestamo}       And
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) >= 0                 AND
						  A.CVE_GPO_EMPRESA            = B.CVE_GPO_EMPRESA AND
						  A.CVE_EMPRESA                = B.CVE_EMPRESA     AND
						  NVL(A.NUM_DIAS_ATRASO_MAX,0) BETWEEN B.NUM_DIAS_ATRASO_MIN AND B.NUM_DIAS_ATRASO_MAX AND
						  B.SIT_CATEGORIA_ATRASO       = 'AC'
				"""){
					curPrestamoIndCatXPres << it.toRowResult()
				}

				// Actualiza la categoria de un prestamo individual
				curPrestamoIndCatXPres.each{ vlBufPrestamo ->
					sql.executeUpdate """
					  UPDATE SIM_PRESTAMO
					      SET CVE_CATEGORIA_ATRASO = ${vlBufPrestamo.CVE_CATEGORIA_ATRASO}
					    WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						  CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
						  ID_PRESTAMO       = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}

				// Actualiza la categoria F para el prestamo individual
				sql.executeUpdate """
				       UPDATE SIM_PRESTAMO
					  SET CVE_CATEGORIA_ATRASO = 'F'
					WHERE CVE_GPO_EMPRESA               = ${pCveGpoEmpresa} AND
					      CVE_EMPRESA                   = ${pCveEmpresa}    AND
					      ID_PRESTAMO                   = ${pIdPrestamo}    AND
					      NVL(CVE_CATEGORIA_ATRASO,'X') = 'E'               AND
					      NVL(NUM_DIAS_ATRASO_ACTUAL,0) > 39
				"""
				//Actualiza las fechas de un prestamo individuaL
		               vgResultado = actualizaFechasPrestamo (pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, vlBEsGrupo,sql);
		}// END switch

	}// END fActualizaInformacionCredito

	//EXPLICARME COMO FUNCIONA EL INTERES MORATORIO
	def pDameInteresMoratorio(pCveGpoEmpresa,
                                    pCveEmpresa,
                                    pIdPrestamo,
                                    pNumPagoAmort,
                                    pFValor,
                                    pInteresMora,
                                    pIVAInteresMora,
                                    pTxRespuesta,
				    sql){

		def vlBufTablaAmortizacion
		def vlTasaMoratoria              
		def vlTasaIVA                    
		def vlFechaCalculoAnt            
		def vlCapitalPromedioMora        
		def vlCapitalActualizadoAnterior 
		def vlImporteAcumulado           
		def vlNumDiasMora                
		def vlNumDiasPeriodo             
		def vlImpDeudaMinima


		def curPagosCapital = []
		

		// Recupera el valor de la deuda minima
		def rowDeudaMinima = sql.firstRow(""" 
			SELECT IMP_DEUDA_MINIMA
			  FROM SIM_PARAMETRO_GLOBAL 
			 WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa} AND
			       CVE_EMPRESA           = ${pCveEmpresa}
		""")
		vlImpDeudaMinima = rowDeudaMinima.IMP_DEUDA_MINIMA	

		//Recupera el registro de la amortizacion correspondiente

		vlBufTablaAmortizacion = sql.firstRow(""" 
			  SELECT FECHA_AMORTIZACION,
				 IMP_CAPITAL_AMORT, IMP_CAPITAL_AMORT_PAGADO
			    FROM SIM_TABLA_AMORTIZACION
			   WHERE CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}   AND
				 CVE_EMPRESA           = ${pCveEmpresa}      AND
				 ID_PRESTAMO           = ${pIdPrestamo}      AND
				 NUM_PAGO_AMORTIZACION = ${pNumPagoAmort}
		""")
             
		// Inicializa datos para calcular el interes moratorio
		pInteresMora          = 0
		pIVAInteresMora       = 0

		// Calcula los dias de mora y en caso de que no haya mora regresa cero
		vlNumDiasMora         = pFValor - vlBufTablaAmortizacion.FECHA_AMORTIZACION
		def vlBufTablaAmortizacion_FECHA_AMORTIZACION = vlBufTablaAmortizacion.FECHA_AMORTIZACION

		//FORMATO DE LAS FECHA AMORTIZACION
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf = new SimpleDateFormat("dd-MM-yyyy");
		vlBufTablaAmortizacion.FECHA_AMORTIZACION = sdf.format(vlBufTablaAmortizacion.FECHA_AMORTIZACION)


		if (vlNumDiasMora <= 0){
			pTxRespuesta = 'No aplican intereses moratorios por que no hay atraso en la fecha'
		}else{
			// Valida que el capital que se adeuda sea mayor a la deuda minima, de lo contrato el interes es cero
			if (vlBufTablaAmortizacion.IMP_CAPITAL_AMORT - vlBufTablaAmortizacion.IMP_CAPITAL_AMORT_PAGADO < vlImpDeudaMinima){
				pTxRespuesta = 'No aplican intereses moratorios por que la deuda de capital es menor a la deuda minima';
			}else{

				// Recupera la tasa de IVA
				def vlBufTasaIva = sql.firstRow(""" 
					 SELECT S.TASA_IVA
					   FROM SIM_PRESTAMO P, SIM_CAT_SUCURSAL S
					  WHERE P.CVE_GPO_EMPRESA   = ${pCveGpoEmpresa}  AND
						P.CVE_EMPRESA       = ${pCveEmpresa}     AND
						P.ID_PRESTAMO       = ${pIdPrestamo}     AND
						P.CVE_GPO_EMPRESA   = S.CVE_GPO_EMPRESA  AND
						P.CVE_EMPRESA       = S.CVE_EMPRESA      AND
						P.ID_SUCURSAL       = S.ID_SUCURSAL
				""")
				vlTasaIVA = vlBufTasaIva.TASA_IVA
				
				// Recupera la tasa moratoria
				// LO SIGUIENTE NO FUNCIONA, HAY QUE REVISAR DEFINICIONES PARA SU IMPLEMENTACION
				vlTasaMoratoria = DameTasaMoratoriaDiaria(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo,sql)

				// Acumula el capital que se debe, en caso de que lo deba por varios dias tiene que multiplicar 
				// por el numero de dias

				sql.eachRow("""       
					   SELECT F_VALOR, SUM(IMP_CAPITAL_PAGADO) AS IMP_CAPITAL_PAGADO
					     FROM (SELECT CASE WHEN NVL(A.F_APLICACION, A.F_LIQUIDACION) <=
							  TO_DATE(${vlBufTablaAmortizacion.FECHA_AMORTIZACION} ,'DD-MM-YYYY')
								THEN TO_DATE(${vlBufTablaAmortizacion.FECHA_AMORTIZACION} ,'DD-MM-YYYY')
								ELSE NVL(A.F_APLICACION, A.F_LIQUIDACION)
							  END AS F_VALOR, B.IMP_CONCEPTO AS IMP_CAPITAL_PAGADO
						     FROM PFIN_MOVIMIENTO A, PFIN_MOVIMIENTO_DET B, PFIN_CAT_OPERACION C
						    WHERE A.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}     AND
							  A.CVE_EMPRESA           = ${pCveEmpresa}        AND
							  A.ID_PRESTAMO           = ${pIdPrestamo}        AND
							  A.NUM_PAGO_AMORTIZACION = ${pNumPagoAmort}      AND              
							  A.SIT_MOVIMIENTO       <> 'CA'               AND
							  A.CVE_GPO_EMPRESA       = B.CVE_GPO_EMPRESA  AND
							  A.CVE_EMPRESA           = B.CVE_EMPRESA      AND
							  A.ID_MOVIMIENTO         = B.ID_MOVIMIENTO    AND
							  B.CVE_CONCEPTO          = 'CAPITA'           AND
							  A.CVE_GPO_EMPRESA       = C.CVE_GPO_EMPRESA  AND
							  A.CVE_EMPRESA           = C.CVE_EMPRESA      AND
							  A.CVE_OPERACION         = C.CVE_OPERACION    AND
							  C.CVE_AFECTA_CREDITO    = 'D'
						    UNION ALL
						    -- Inserta un registro para la fecha de amortizacion
						   SELECT TO_DATE(${vlBufTablaAmortizacion.FECHA_AMORTIZACION} ,'DD-MM-YYYY')
							 AS F_VALOR, 0 AS IMP_CAPITAL_PAGADO
						     FROM DUAL
						    UNION ALL
						    -- Inserta un registro para la fecha en la que se esta realizando el pago
						   SELECT TO_DATE(${vlBufTablaAmortizacion.FECHA_AMORTIZACION} ,'DD-MM-YYYY') 
							AS F_VALOR, 0 AS IMP_CAPITAL_PAGADO
						     FROM DUAL)
					    GROUP BY F_VALOR
					    ORDER BY F_VALOR
				"""){
				  curPagosCapital << it.toRowResult()
				}
				//Acumula el capital que se debe, en caso de que lo deba por varios dias tiene que 
				//multiplicar por el numero de dias
				curPagosCapital.each{ vlBufPagos ->
					// Inicializa el capital pagado a la fecha de amortizacion
					if (vlBufPagos.F_VALOR == vlBufTablaAmortizacion_FECHA_AMORTIZACION){
						vlCapitalActualizadoAnterior = vlBufTablaAmortizacion.IMP_CAPITAL_AMORT -
							vlBufPagos.IMP_CAPITAL_PAGADO;
						vlImporteAcumulado = 0
						vlFechaCalculoAnt  = vlBufTablaAmortizacion_FECHA_AMORTIZACION
					}else{
						vlNumDiasPeriodo   = vlBufPagos.F_VALOR - vlFechaCalculoAnt;
						vlImporteAcumulado = vlImporteAcumulado + (vlCapitalActualizadoAnterior * vlNumDiasPeriodo);

						// Actualiza los acumulados
						vlCapitalActualizadoAnterior = vlCapitalActualizadoAnterior - vlBufPagos.IMP_CAPITAL_PAGADO;
						vlFechaCalculoAnt            = vlBufPagos.F_VALOR;
					}
				}
				vlCapitalPromedioMora    = vlImporteAcumulado / vlNumDiasMora;

				pInteresMora             = vlImporteAcumulado * vlTasaMoratoria * vlNumDiasMora;
				pIVAInteresMora          = pInteresMora * vlTasaIVA;

			}
		}
		def interesMora = ['pInteresMora' : pInteresMora , 'pIVAInteresMora' : pIVAInteresMora]
		return interesMora
	}

	def DameTasaMoratoriaDiaria(pCveGpoEmpresa,
                                     pCveEmpresa,
                                     pIdPrestamo,
				     sql){
		//P.TIPO_TASA_RECARGO = 'Fija independiente' NO CORRESPONDE CON LA LONGITUD DEL CAMPO
		//PARA EL CALCULO DE LA TASA MORATORIA ES INDISPENSABLE UTILIZAR SI ESTA INDEXADA O NO INDEXADA A UN PAPEL?
		//REVISAR COMO FUNCIONA LA TASA MORATORIA DIARIA ************************************************************
		def vlBufTasaIntMora = sql.firstRow(""" 
			SELECT NVL(CASE WHEN P.ID_TIPO_RECARGO IN (3,5) 
					     THEN -- Si el tipo de recargo implica interés moratorio regresa el valor de la tasa
				             CASE WHEN P.TIPO_TASA_RECARGO = 'Fija independiente' THEN 
				                       ROUND(DECODE(P.TIPO_TASA,'No indexada', P.TASA_RECARGO, T.VALOR)/100/
			    			 		                 DECODE(P.TIPO_TASA,'No indexada', PT.DIAS, TRV.DIAS) ,20)
				                  ELSE P.VALOR_TASA * P.FACTOR_TASA_RECARGO
				             END
				        ELSE 0 -- Si el recargo no es de tipo interés moratorio regresa 0 en el valor de la tasa
				   END, 0) AS TASA_INTERES_MORATORIO
			  FROM SIM_PRESTAMO P, SIM_CAT_SUCURSAL S, SIM_CAT_PERIODICIDAD PT, SIM_CAT_PERIODICIDAD TRV, 
			       SIM_CAT_TASA_REFER_DETALLE T, SIM_CAT_TASA_REFERENCIA TR
			  WHERE P.CVE_GPO_EMPRESA 	            = ${pCveGpoEmpresa}
			    AND P.CVE_EMPRESA     	            = ${pCveEmpresa}
			    AND P.ID_PRESTAMO     	            = ${pIdPrestamo}
			    AND P.CVE_GPO_EMPRESA 	            = S.CVE_GPO_EMPRESA
			    AND P.CVE_EMPRESA     	            = S.CVE_EMPRESA
			    AND P.ID_SUCURSAL                   = S.ID_SUCURSAL
			    --  Periodicidad de la tasa de recargo
			    AND P.CVE_GPO_EMPRESA 	            = PT.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	            = PT.CVE_EMPRESA(+)
			    AND P.ID_PERIODICIDAD_TASA_RECARGO  = PT.ID_PERIODICIDAD(+)
			    --  Relación con la tasa de referencia
			    AND P.CVE_GPO_EMPRESA 	            = TR.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	            = TR.CVE_EMPRESA(+)
			    AND P.ID_TASA_REFERENCIA_RECARGO    = TR.ID_TASA_REFERENCIA(+)
			    --  Relación con el detalle de la tasa de referencia
			    AND TR.CVE_GPO_EMPRESA 	            = TRV.CVE_GPO_EMPRESA(+)
			    AND TR.CVE_EMPRESA     		= TRV.CVE_EMPRESA(+)
			    AND TR.ID_PERIODICIDAD              = TRV.ID_PERIODICIDAD(+)
			    --  Relación con la tasa de referencia
			    AND P.CVE_GPO_EMPRESA 	            = T.CVE_GPO_EMPRESA(+)
			    AND P.CVE_EMPRESA     	            = T.CVE_EMPRESA(+)
			    AND P.ID_TASA_REFERENCIA_RECARGO    = T.ID_TASA_REFERENCIA(+)
			    -- Se obtiene el máximo de id referencia detalle, esto es la tasa más actual
			    AND PKG_CREDITO.dameidtasarefdet(P.CVE_GPO_EMPRESA,P.CVE_EMPRESA,
				P.ID_TASA_REFERENCIA_RECARGO) = T.ID_TASA_REFERENCIA_DETALLE(+)
		""")
		def vlTasaIntMora = vlBufTasaIntMora.TASA_INTERES_MORATORIO

	}
   
	def actualizaDiasAntiguedad (pCveGpoEmpresa,
                                      pCveEmpresa,
                                      pFValor,
                                      pImpDeudaMinima,
				      sql){

		def vlFechaDiasAntiguedad
		def vlDiasAntiguedad
		def vlValorEncontrado
		def vlIdDistribucionPago
		def vlSumaPagosCapital
		def vlIdPrestamo


	}

	def actualizaFechasPrestamo (pCveGpoEmpresa,
                                      pCveEmpresa,
                                      pIdPrestamo,
                                      pBGrupo,
				      sql){

		// Elige el cursor de un prestamo o de todos, si el parametro es cero significa que debe actualizar todos los prestamos
		if (pIdPrestamo == 0){

			def curPrestamos = []		//Recupera las tablas de amortizacion de un prestamo grupal
		def curPorGpoPrestamo = []
		sql.eachRow("""
			   SELECT T.CVE_GPO_EMPRESA, T.CVE_EMPRESA, T.ID_PRESTAMO, T.NUM_PAGO_AMORTIZACION, 
				  ROUND(NVL(P.MONTO_FIJO_PERIODO,0),2) AS IMP_PAGO_TARDIO
			     FROM SIM_PRESTAMO_GPO_DET G, SIM_TABLA_AMORTIZACION T, SIM_PRESTAMO P
			    WHERE G.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
			      AND G.CVE_EMPRESA           = ${pCveEmpresa}
			      AND G.ID_PRESTAMO_GRUPO     = ${pIdPrestamo}
			      AND G.CVE_GPO_EMPRESA       = T.CVE_GPO_EMPRESA
			      AND G.CVE_EMPRESA           = T.CVE_EMPRESA
			      AND G.ID_PRESTAMO           = T.ID_PRESTAMO
			      AND T.FECHA_AMORTIZACION    < ${pFValor}
			      AND T.B_PAGADO              = ${cFalso}
			      AND T.CVE_GPO_EMPRESA       = P.CVE_GPO_EMPRESA
			      AND T.CVE_EMPRESA           = P.CVE_EMPRESA
			      AND T.ID_PRESTAMO           = P.ID_PRESTAMO
			      AND P.ID_TIPO_RECARGO    IN (4,5)
			      AND NVL(P.MONTO_FIJO_PERIODO,0) > 0
		""") {
		  curPorGpoPrestamo << it.toRowResult()
		}

			sql.eachRow("""	
				SELECT CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO,
				       --OBTIENE LA ULTIMA FECHA DE AMORTIZACION QUE DEBE DE PAGARSE PARA UN PRESTAMO
				       MAX(CASE WHEN B_PAGADO = 'F' THEN FECHA_AMORTIZACION ELSE NULL END) AS F_ULT_AMORTIZACION,           
				       --OBTIENE LA FECHA DEL ULTIMO PAGO REALIZADO
				       MAX(FECHA_AMORT_PAGO_ULTIMO) AS F_ULT_PAGO_REALIZADO,
				       --OBTIENE LA PROXIMA FECHA DE AMORTIZACION QUE DEBE PAGARSE
				       MIN(CASE WHEN B_PAGADO = 'F' THEN FECHA_AMORTIZACION ELSE NULL END) AS F_PROX_PAGO
				  FROM SIM_TABLA_AMORTIZACION 
				 WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
				       CVE_EMPRESA     = ${pCveEmpresa}
				 GROUP BY CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO
			"""){
				curPrestamos << it.toRowResult()
			}

			curPrestamos.each{ vlBufPrestamo ->
				// Actualiza todos los prestamos
				sql.executeUpdate """
				     UPDATE SIM_PRESTAMO
					SET F_ULT_PAGO_REALIZADO = ${vlBufPrestamo.F_ULT_PAGO_REALIZADO},
					    F_PROX_PAGO          = ${vlBufPrestamo.F_PROX_PAGO},
					    F_ULT_AMORTIZACION   = ${vlBufPrestamo.F_ULT_AMORTIZACION}
				      WHERE CVE_GPO_EMPRESA = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
					    CVE_EMPRESA     = ${vlBufPrestamo.CVE_EMPRESA}     AND
					    ID_PRESTAMO     = ${vlBufPrestamo.ID_PRESTAMO}
				"""		
			}

		}else{
			if (pBGrupo == cFalso){

				def curPrestamo = []
				sql.eachRow("""
					SELECT CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO,
					       MAX(CASE WHEN B_PAGADO = 'F' THEN FECHA_AMORTIZACION ELSE NULL END) AS F_ULT_AMORTIZACION,           
					       MAX(FECHA_AMORT_PAGO_ULTIMO) AS F_ULT_PAGO_REALIZADO,
					       MIN(CASE WHEN B_PAGADO = 'F' THEN FECHA_AMORTIZACION ELSE NULL END) AS F_PROX_PAGO
					  FROM SIM_TABLA_AMORTIZACION 
					 WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa} AND
					       CVE_EMPRESA     = ${pCveEmpresa}    AND
					       ID_PRESTAMO     = ${pIdPrestamo}
					 GROUP BY CVE_GPO_EMPRESA, CVE_EMPRESA, ID_PRESTAMO
				"""){
					curPrestamo << it.toRowResult()
				}

				// Actualiza un prestamo
				curPrestamo.each{ vlBufPrestamo ->
					sql.executeUpdate """
						UPDATE SIM_PRESTAMO
						   SET F_ULT_PAGO_REALIZADO = ${vlBufPrestamo.F_ULT_PAGO_REALIZADO},
						       F_PROX_PAGO          = ${vlBufPrestamo.F_PROX_PAGO},
						       F_ULT_AMORTIZACION   = ${vlBufPrestamo.F_ULT_AMORTIZACION}
						 WHERE CVE_GPO_EMPRESA = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						       CVE_EMPRESA     = ${vlBufPrestamo.CVE_EMPRESA}     AND
						       ID_PRESTAMO     = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}
			}else{

				def curPrestamoGpo = []
				sql.eachRow("""
					SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO,
					       MAX(CASE WHEN B_PAGADO = 'F' THEN FECHA_AMORTIZACION ELSE NULL END) AS F_ULT_AMORTIZACION,           
					       MAX(FECHA_AMORT_PAGO_ULTIMO) AS F_ULT_PAGO_REALIZADO,
					       MIN(CASE WHEN B_PAGADO = 'F' THEN FECHA_AMORTIZACION ELSE NULL END) AS F_PROX_PAGO
					  FROM SIM_PRESTAMO_GPO_DET A, SIM_TABLA_AMORTIZACION B
					 WHERE A.CVE_GPO_EMPRESA   = ${pCveGpoEmpresa}    AND
					       A.CVE_EMPRESA       = ${pCveEmpresa}       AND
					       A.ID_PRESTAMO_GRUPO = ${pIdPrestamo}       AND
					       A.CVE_GPO_EMPRESA   = B.CVE_GPO_EMPRESA AND
					       A.CVE_EMPRESA       = B.CVE_EMPRESA     AND
					       A.ID_PRESTAMO       = B.ID_PRESTAMO
					 GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO
				"""){
					curPrestamoGpo << it.toRowResult()
				}
				// Actualiza los prestamos de un grupo
				curPrestamoGpo.each{ vlBufPrestamo ->
					sql.executeUpdate """
						UPDATE SIM_PRESTAMO
						   SET F_ULT_PAGO_REALIZADO = ${vlBufPrestamo.F_ULT_PAGO_REALIZADO},
						       F_PROX_PAGO          = ${vlBufPrestamo.F_PROX_PAGO},
						       F_ULT_AMORTIZACION   = ${vlBufPrestamo.F_ULT_AMORTIZACION}
						 WHERE CVE_GPO_EMPRESA = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
						       CVE_EMPRESA     = ${vlBufPrestamo.CVE_EMPRESA}     AND
						       ID_PRESTAMO     = ${vlBufPrestamo.ID_PRESTAMO}
					"""
				}
			}
		}
	}


	def actualizaFechasPrestamoGpo (pCveGpoEmpresa,
                                      pCveEmpresa,
                                      pIdPrestamo,
				      sql){



		// Elige el cursor de un prestamo o de todos, si el parametro es cero significa que debe actualizar todos los prestamos
		if (pIdPrestamo == 0){

			def curPrestamos = []
			sql.eachRow("""	
				SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO AS ID_PRESTAMO,
				       MAX(B.F_ULT_AMORTIZACION) AS F_ULT_AMORTIZACION,           
				       MAX(B.F_ULT_PAGO_REALIZADO) AS F_ULT_PAGO_REALIZADO,
				       MIN(B.F_PROX_PAGO) AS F_PROX_PAGO
				  FROM SIM_PRESTAMO_GPO_DET A, SIM_PRESTAMO B
				 WHERE A.CVE_GPO_EMPRESA = ${pCveGpoEmpresa}    AND
				       A.CVE_EMPRESA     = ${pCveEmpresa}       AND
				       A.CVE_GPO_EMPRESA = B.CVE_GPO_EMPRESA AND
				       A.CVE_EMPRESA     = B.CVE_EMPRESA     AND
				       A.ID_PRESTAMO     = B.ID_PRESTAMO
				 GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO
			"""){
				curPrestamos << it.toRowResult()
			}


			curPrestamos.each{ vlBufPrestamo ->
				// Actualiza todos los prestamos
				sql.executeUpdate """
				    UPDATE SIM_PRESTAMO_GRUPO
					SET F_ULT_PAGO_REALIZADO = ${vlBufPrestamo.F_ULT_PAGO_REALIZADO},
					    F_PROX_PAGO          = ${vlBufPrestamo.F_PROX_PAGO},
					    F_ULT_AMORTIZACION   = ${vlBufPrestamo.F_ULT_AMORTIZACION}
				      WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
					    CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
					    ID_PRESTAMO_GRUPO = ${vlBufPrestamo.ID_PRESTAMO}
				"""
			}
		}else{

			def curPrestamo = []
			sql.eachRow("""
				SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO AS ID_PRESTAMO,
				       MAX(B.F_ULT_AMORTIZACION) AS F_ULT_AMORTIZACION,           
				       MAX(B.F_ULT_PAGO_REALIZADO) AS F_ULT_PAGO_REALIZADO,
				       MIN(B.F_PROX_PAGO) AS F_PROX_PAGO
				  FROM SIM_PRESTAMO_GPO_DET A, SIM_PRESTAMO B
				 WHERE A.CVE_GPO_EMPRESA   = ${pCveGpoEmpresa}  AND
				       A.CVE_EMPRESA       = ${pCveEmpresa}     AND
				       A.ID_PRESTAMO_GRUPO = ${pIdPrestamo}     AND
				       A.CVE_GPO_EMPRESA = B.CVE_GPO_EMPRESA AND
				       A.CVE_EMPRESA     = B.CVE_EMPRESA     AND
				       A.ID_PRESTAMO     = B.ID_PRESTAMO
				 GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO
			"""){
				curPrestamo << it.toRowResult()
			}

			// Actualiza un prestamo de grupo
			curPrestamo.each{ vlBufPrestamo ->
				sql.executeUpdate """
				    UPDATE SIM_PRESTAMO_GRUPO
					SET F_ULT_PAGO_REALIZADO = ${vlBufPrestamo.F_ULT_PAGO_REALIZADO},
					    F_PROX_PAGO          = ${vlBufPrestamo.F_PROX_PAGO},
					    F_ULT_AMORTIZACION   = ${vlBufPrestamo.F_ULT_AMORTIZACION}
				      WHERE CVE_GPO_EMPRESA   = ${vlBufPrestamo.CVE_GPO_EMPRESA} AND
					    CVE_EMPRESA       = ${vlBufPrestamo.CVE_EMPRESA}     AND
					    ID_PRESTAMO_GRUPO = ${vlBufPrestamo.ID_PRESTAMO}
				"""
			}
		}
	}

	//Funcion que determina la proporcion del pago de acuerdo a un importe
	def fCalculaProporcion(pCveGpoEmpresa,
                                pCveEmpresa,
                                pIdPrestamo,
                                pFValor,
                                pImpPago,
                                pTxRespuesta,
				sql){

		def vlBEsGrupo
		def vlResultado
		def vlSaldo
		def vlSaldoProrrateo
		def vlImpAmortizacion
		def vlNumAmortProrrateo
		def vlCurSalida
		def vlImpVariaProporcion
		def vlImpProrrateado
		def vlImpAjuste

		// CURSOR QUE OBTIENE LAS AMORTIZACIONES DEL PRESTAMO GRUPAL PARA OBTENER LA DEUDA DE CADA AMORTIZACION
		// Cursor que devuelve las amortizaciones del prestamo para todos los integrantes de un grupo y ordenadas por
		// el numero de amortizacion, esto seria por orden cronologico
		def curAmortizaciones = []
		sql.eachRow("""
			 SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO, B.NUM_PAGO_AMORTIZACION, SUM(IMP_NETO * -1) AS IMP_DEUDA
			   FROM SIM_PRESTAMO_GPO_DET A, V_SIM_TABLA_AMORT_CONCEPTO B
			  WHERE A.CVE_GPO_EMPRESA   = ${pCveGpoEmpresa}
			    AND A.CVE_EMPRESA       = ${pCveEmpresa}
			    AND A.ID_PRESTAMO_GRUPO = ${pIdPrestamo}
			    AND A.CVE_GPO_EMPRESA   = B.CVE_GPO_EMPRESA
			    AND A.CVE_EMPRESA       = B.CVE_EMPRESA
			    AND A.ID_PRESTAMO       = B.ID_PRESTAMO
			  GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO_GRUPO, B.NUM_PAGO_AMORTIZACION
			  ORDER BY B.NUM_PAGO_AMORTIZACION
		"""){
			curAmortizaciones << it.toRowResult()
		}

		//Valida que el credito sea grupal
		def esGrupal = sql.firstRow("""
			  SELECT ID_PRESTAMO_GRUPO
			    FROM SIM_PRESTAMO_GRUPO
			   WHERE CVE_GPO_EMPRESA   = ${pCveGpoEmpresa}
			     AND CVE_EMPRESA       = ${pCveEmpresa}
			     AND ID_PRESTAMO_GRUPO = ${pIdPrestamo}		
		""")
		
		if (esGrupal?.ID_PRESTAMO_GRUPO){
			vlBEsGrupo = cVerdadero

			// Actualiza la informacion del credito a la fecha valor solicitada
			// EXISTE UN PROBLEMA CON EL CAMPO pFValor
			// vlResultado = fActualizaInformacionCredito(pCveGpoEmpresa, pCveEmpresa, pIdPrestamo, pFValor, pTxRespuesta, sql)

			// Determina lo siguiente:
			// Amortizaciones que pueden cubrirse completamente con el pago Sobrante
			// Amortizacion en la que se prorrateara el pago

			// Inicializa saldo
			vlSaldo           = pImpPago;
			vlSaldoProrrateo  = 0;
			vlImpAmortizacion = 0;

			// Inicializa la amortizacion que se prorratea a un numero muy grande
			vlNumAmortProrrateo = 99999;
			try{
				curAmortizaciones.each{ vlAmortizaciones ->
					vlSaldo = vlSaldo - vlAmortizaciones.IMP_DEUDA;
					if (vlSaldo < 0){
						vlNumAmortProrrateo = vlAmortizaciones.NUM_PAGO_AMORTIZACION;
				     		vlSaldoProrrateo    = vlSaldo + vlAmortizaciones.IMP_DEUDA;
						throw new Exception("return from closure") 
				  	}
				}
			} catch (Exception e) { }


			//Calcula el importe de la amortizacion a prorratear
			def importeProrrateo = sql.firstRow("""
			      SELECT SUM(IMP_NETO * -1) IMPORTE
				FROM SIM_PRESTAMO_GPO_DET A, V_SIM_TABLA_AMORT_CONCEPTO B
			       WHERE A.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa}
				 AND A.CVE_EMPRESA           = ${pCveEmpresa}
				 AND A.ID_PRESTAMO_GRUPO     = ${pIdPrestamo}
				 AND A.CVE_GPO_EMPRESA       = B.CVE_GPO_EMPRESA
				 AND A.CVE_EMPRESA           = B.CVE_EMPRESA
				 AND A.ID_PRESTAMO           = B.ID_PRESTAMO
				 AND B.NUM_PAGO_AMORTIZACION = ${vlNumAmortProrrateo}
			""")
			vlImpAmortizacion = importeProrrateo.IMPORTE

			// Despliega el importe para cada participante del credito y calcula el valor de la suma de las proporciones
			vlImpProrrateado = 0; 


			// Cursor que devuelve el monto del pago que se asignara a cada participante
			// Se suma las amortizaciones pagadas totalmente mas la proporcion de la amortizacion final
			def curProporcion = []
			sql.eachRow("""
				 SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.ID_CLIENTE, C.NOM_COMPLETO, SUM(IMP_DEUDA) AS IMP_DEUDA
				   FROM (SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO, ROUND(SUM(IMP_NETO *-1),2) AS IMP_DEUDA
					   FROM SIM_PRESTAMO_GPO_DET A, V_SIM_TABLA_AMORT_CONCEPTO B
					  WHERE A.CVE_GPO_EMPRESA   = ${pCveGpoEmpresa} 
					    AND A.CVE_EMPRESA       = ${pCveEmpresa}
					    AND A.ID_PRESTAMO_GRUPO = ${pIdPrestamo}
					    AND A.CVE_GPO_EMPRESA   = B.CVE_GPO_EMPRESA
					    AND A.CVE_EMPRESA       = B.CVE_EMPRESA
					    AND A.ID_PRESTAMO       = B.ID_PRESTAMO
					    AND B.NUM_PAGO_AMORTIZACION < ${vlNumAmortProrrateo}
					  GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO
					  UNION ALL
					 SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO, 
						ROUND(SUM(IMP_NETO *-1) / ${vlImpAmortizacion} * ${vlSaldoProrrateo},2) AS IMP_DEUDA
					   FROM SIM_PRESTAMO_GPO_DET A, V_SIM_TABLA_AMORT_CONCEPTO B
					  WHERE A.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa} 
					    AND A.CVE_EMPRESA           = ${pCveEmpresa}
					    AND A.ID_PRESTAMO_GRUPO     = ${pIdPrestamo}
					    AND A.CVE_GPO_EMPRESA       = B.CVE_GPO_EMPRESA
					    AND A.CVE_EMPRESA           = B.CVE_EMPRESA
					    AND A.ID_PRESTAMO           = B.ID_PRESTAMO
					    AND B.NUM_PAGO_AMORTIZACION = ${vlNumAmortProrrateo}
					  GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO) A, SIM_PRESTAMO B, RS_GRAL_PERSONA C
				  WHERE A.CVE_GPO_EMPRESA = B.CVE_GPO_EMPRESA AND
					A.CVE_EMPRESA     = B.CVE_EMPRESA     AND
					A.ID_PRESTAMO     = B.ID_PRESTAMO     AND
					B.CVE_GPO_EMPRESA = C.CVE_GPO_EMPRESA AND
					B.CVE_EMPRESA     = C.CVE_EMPRESA     AND
					B.ID_CLIENTE      = C.ID_PERSONA
				  GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.ID_CLIENTE, C.NOM_COMPLETO
			"""){
				curProporcion << it.toRowResult()
			}

			curProporcion.each{ vlProporcion ->
				// Despliega la informacion
				vlImpProrrateado = vlImpProrrateado + vlProporcion.IMP_DEUDA
			}

			// Determina el importe a ajustar en la proporcion de acuerdo al parametro
			vlImpAjuste = 0;

			if ( (Math.abs(pImpPago) - Math.abs(vlImpProrrateado)) > vlImpVariaProporcion ){
				vlImpAjuste = pImpPago - vlImpProrrateado;
			}

			curProporcion = []
			sql.eachRow("""


         SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.ID_CLIENTE, C.NOM_COMPLETO, 
                RANK() OVER (PARTITION BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.ID_CLIENTE ORDER BY SUM(IMP_DEUDA) DESC) AS ID_ORDEN,
                CASE WHEN RANK() OVER (PARTITION BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.ID_CLIENTE ORDER BY SUM(IMP_DEUDA) DESC) = 1
                     THEN SUM(IMP_DEUDA) + ${vlImpAjuste}
                     ELSE SUM(IMP_DEUDA)
                END AS IMP_DEUDA
           FROM (SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO, ROUND(SUM(IMP_NETO *-1),2) AS IMP_DEUDA
                   FROM SIM_PRESTAMO_GPO_DET A, V_SIM_TABLA_AMORT_CONCEPTO B
                  WHERE A.CVE_GPO_EMPRESA   = ${pCveGpoEmpresa} 
                    AND A.CVE_EMPRESA       = ${pCveEmpresa}
                    AND A.ID_PRESTAMO_GRUPO = ${pIdPrestamo}
                    AND A.CVE_GPO_EMPRESA   = B.CVE_GPO_EMPRESA
                    AND A.CVE_EMPRESA       = B.CVE_EMPRESA
                    AND A.ID_PRESTAMO       = B.ID_PRESTAMO
                    AND B.NUM_PAGO_AMORTIZACION < ${vlNumAmortProrrateo}
                  GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO
                  UNION ALL
                 SELECT A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO, 
                        ROUND(SUM(IMP_NETO *-1) / ${vlImpAmortizacion} * ${vlSaldoProrrateo},2) AS IMP_DEUDA
                   FROM SIM_PRESTAMO_GPO_DET A, V_SIM_TABLA_AMORT_CONCEPTO B
                  WHERE A.CVE_GPO_EMPRESA       = ${pCveGpoEmpresa} 
                    AND A.CVE_EMPRESA           = ${pCveEmpresa}
                    AND A.ID_PRESTAMO_GRUPO     = ${pIdPrestamo}
                    AND A.CVE_GPO_EMPRESA       = B.CVE_GPO_EMPRESA
                    AND A.CVE_EMPRESA           = B.CVE_EMPRESA
                    AND A.ID_PRESTAMO           = B.ID_PRESTAMO
                    AND B.NUM_PAGO_AMORTIZACION = ${vlNumAmortProrrateo}
                  GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, B.ID_PRESTAMO) A, SIM_PRESTAMO B, RS_GRAL_PERSONA C
          WHERE A.CVE_GPO_EMPRESA = B.CVE_GPO_EMPRESA AND
                A.CVE_EMPRESA     = B.CVE_EMPRESA     AND
                A.ID_PRESTAMO     = B.ID_PRESTAMO     AND
                B.CVE_GPO_EMPRESA = C.CVE_GPO_EMPRESA AND
                B.CVE_EMPRESA     = C.CVE_EMPRESA     AND
                B.ID_CLIENTE      = C.ID_PERSONA
          GROUP BY A.CVE_GPO_EMPRESA, A.CVE_EMPRESA, A.ID_PRESTAMO, B.ID_CLIENTE, C.NOM_COMPLETO

			"""){
				curProporcion << it.toRowResult()
			}
			return curProporcion


		}
	}
}
