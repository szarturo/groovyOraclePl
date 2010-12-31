//PKG_PROCESADOR_FINANCIERO
class PKG_PROCESADOR_FINANCIERO {

    def cSitPreMovtoNoProcesado         = 'NP'
    def cSitPreMovtoProcesadoVirtual    = 'PV'
    def cSitPreMovtoProcesadoReal       = 'PR'
    def cSitPreMovtoCancelado           = 'CA'
    def vgFechaSistema


    String toString(){"PKG_PROCESADOR_FINANCIERO"}

    def AsignaFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql){
	    def row = sql.firstRow("""
		       SELECT  TO_CHAR(TO_DATE(F_MEDIO,'DD-MM-YYYY'),'DD-MON-YYYY')  F_MEDIO
			FROM    PFIN_PARAMETRO
			WHERE   CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
			    AND CVE_EMPRESA     = ${pCveEmpresa}
			    AND CVE_MEDIO       = 'SYSTEM' """)
	    return row.F_MEDIO
    }

   
    def pProcesaMovimiento(
		pCveGpoEmpresa,
                pCveEmpresa,
                pIdPremovimiento,
                pSitMovimiento,
                pCveUsuarioCancela,
                pBDebug,
                pTxrespuesta,
		sql){

	def vlBufPreMovto
	def vlBufOperacion
	def vlIdMovimiento
	def vlValidaFecha

	vgFechaSistema = AsignaFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql)

	//Se obtiene el registro del premovimiento, 
	vlBufPreMovto = sql.firstRow(""" 
					SELECT  *
					FROM    PFIN_PRE_MOVIMIENTO
					WHERE   CVE_GPO_EMPRESA         = ${pCveGpoEmpresa}
						AND CVE_EMPRESA         = ${pCveEmpresa}
						AND ID_PRE_MOVIMIENTO   = ${pIdPremovimiento} 
					""")

	//Se valida que la actualización sea correcta
	if (! (vlBufPreMovto.SIT_PRE_MOVIMIENTO == cSitPreMovtoNoProcesado && 
              (pSitMovimiento == cSitPreMovtoProcesadoVirtual || pSitMovimiento == cSitPreMovtoProcesadoReal))
	   || (vlBufPreMovto.SIT_PRE_MOVIMIENTO == cSitPreMovtoProcesadoVirtual && 
              (pSitMovimiento == cSitPreMovtoCancelado || pSitMovimiento == cSitPreMovtoProcesadoReal))){
		//println "Tipo de actualización ilegal de ${vlBufPreMovto.SIT_PRE_MOVIMIENTO} a ${pSitMovimiento}"
	}else{
		//ACTUALIZACIONES LEGALES
		//NP -> (PV,PR)
		//PV -> (CA,PR)

		if (pSitMovimiento!='CA'){
		    
		    // Se obtiene la configuración de la clave de operación
		    vlBufOperacion = sql.firstRow(""" 
			SELECT *
			       FROM PFIN_CAT_OPERACION
			       WHERE CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
				 AND CVE_EMPRESA     = ${pCveEmpresa}
				 AND CVE_OPERACION   = ${vlBufPreMovto.CVE_OPERACION}
			""")

		    //AL PARECER NUNCA ES UTILIZADO
		    //IF vlBufPreMovto.F_OPERACION < vgFechaSistema THEN     

		    //Se obtiene el id del movimiento 
		    def rowIdMovimiento = sql.firstRow("SELECT SQ01_PFIN_MOVIMIENTO.NEXTVAL as ID_MOVIMIENTO FROM DUAL")
		    vlIdMovimiento = rowIdMovimiento.ID_MOVIMIENTO

		    sql.execute("""
			INSERT  INTO PFIN_MOVIMIENTO(        
		            CVE_GPO_EMPRESA, CVE_EMPRESA, ID_MOVIMIENTO, ID_CUENTA, CVE_DIVISA, F_OPERACION, F_LIQUIDACION, 
		            CVE_OPERACION, IMP_NETO, PREC_OPERACION, TIPO_CAMBIO, ID_REFERENCIA, ID_PRESTAMO, CVE_MERCADO, 
		            CVE_MEDIO, TX_NOTA, TX_REFERENCIA, ID_GRUPO, ID_PRE_MOVIMIENTO, SIT_MOVIMIENTO, FH_REGISTRO, 
		            FH_ACTIVACION, LOG_IP_ADDRESS, LOG_OS_USER, LOG_HOST, CVE_USUARIO, F_APLICACION, NUM_PAGO_AMORTIZACION)
		            VALUES  (
		            ${vlBufPreMovto.CVE_GPO_EMPRESA}, 
		            ${vlBufPreMovto.CVE_EMPRESA}, 
		            ${vlIdMovimiento}, 
		            ${vlBufPreMovto.ID_CUENTA}, 
		            ${vlBufPreMovto.CVE_DIVISA}, 
		            ${vlBufPreMovto.F_OPERACION}, 
		            ${vlBufPreMovto.F_LIQUIDACION}, 
		            ${vlBufPreMovto.CVE_OPERACION}, 
		            ${vlBufPreMovto.IMP_NETO}, 
		            ${vlBufPreMovto.PREC_OPERACION}, 
		            ${vlBufPreMovto.TIPO_CAMBIO}, 
		            ${vlBufPreMovto.ID_REFERENCIA}, 
		            ${vlBufPreMovto.ID_PRESTAMO}, 
		            ${vlBufPreMovto.CVE_MERCADO}, 
		            ${vlBufPreMovto.CVE_MEDIO}, 
		            ${vlBufPreMovto.TX_NOTA}, 
		            ${vlBufPreMovto.TX_REFERENCIA}, 
		            ${vlBufPreMovto.ID_GRUPO}, 
		            ${vlBufPreMovto.ID_PRE_MOVIMIENTO}, 
		            ${cSitPreMovtoProcesadoVirtual}, 
		            ${vlBufPreMovto.FH_REGISTRO}, 
		            ${vlBufPreMovto.FH_ACTIVACION}, 
		            ${vlBufPreMovto.LOG_IP_ADDRESS}, 
		            ${vlBufPreMovto.LOG_OS_USER}, 
		            ${vlBufPreMovto.LOG_HOST}, 
		            ${vlBufPreMovto.CVE_USUARIO},
		            NVL(${vlBufPreMovto.F_APLICACION}, ${vlBufPreMovto.F_LIQUIDACION}),
		            ${vlBufPreMovto.NUM_PAGO_AMORTIZACION})
			""")

                    //Se inserta el detalle de la operación
		    sql.execute("""                
				INSERT INTO PFIN_MOVIMIENTO_DET
				SELECT CVE_GPO_EMPRESA, CVE_EMPRESA, ${vlIdMovimiento}, CVE_CONCEPTO, IMP_CONCEPTO, TX_NOTA 
				  FROM PFIN_PRE_MOVIMIENTO_DET
				 WHERE CVE_GPO_EMPRESA     = ${pCveGpoEmpresa}
				   AND CVE_EMPRESA         = ${pCveEmpresa}
				   AND ID_PRE_MOVIMIENTO   = ${pIdPremovimiento}
				""")
		    //Se modifica la situación del premovimiento
		    sql.executeUpdate """
			    UPDATE PFIN_PRE_MOVIMIENTO SET
				   SIT_PRE_MOVIMIENTO  = ${cSitPreMovtoProcesadoVirtual},
				   ID_MOVIMIENTO       = ${vlIdMovimiento}
			     WHERE CVE_GPO_EMPRESA     = ${pCveGpoEmpresa}
			       AND CVE_EMPRESA         = ${pCveEmpresa}
			       AND ID_PRE_MOVIMIENTO   = ${pIdPremovimiento}
				"""
		}else{
		    // En caso de ser una cancelación solo se actualiza la situacion del movimiento y premovimiento
		    // Se modifica la situación del premovimiento            
		}

		if (vlBufOperacion.CVE_AFECTA_SALDO=='I' || vlBufOperacion.CVE_AFECTA_SALDO=='D'){
		    
		    //VERIFICA DE QUE FORMA VA A AFECTAR EL SALDO DEL CLIENTE
		    def afectaSaldo = 0
		    
		    if (pSitMovimiento!='CA'){ 
		        afectaSaldo = vlBufOperacion.CVE_AFECTA_SALDO == 'I' ? 1 : -1
		    }else{
		        afectaSaldo = vlBufOperacion.CVE_AFECTA_SALDO == 'I' ? -1 : 1
		    }

		    //Se afecta el saldo del cliente
		    def rowSaldo = sql.firstRow("""
						SELECT ID_CUENTA FROM PFIN_SALDO 
		                                 WHERE  CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
		                                    AND CVE_EMPRESA     = ${pCveEmpresa}
		                                    AND F_FOTO          = ${vlBufPreMovto.F_OPERACION}
		                                    AND ID_CUENTA       = ${vlBufPreMovto.ID_CUENTA}
		                                    AND CVE_DIVISA      = ${vlBufPreMovto.CVE_DIVISA}
					""")
		    if (rowSaldo){
			//YA EXISTE LA CUENTA
		        //ACTUALIZA EL SALDO DE LA CUENTA CON LA FOTO
		        //¿PORQUE EL SALDO LO LLEVA POR FECHA?
		        sql.executeUpdate """
		            UPDATE  PFIN_SALDO SET
		                    SDO_EFECTIVO    = SDO_EFECTIVO + (${vlBufPreMovto.IMP_NETO} * ${afectaSaldo})
		            WHERE   CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
		                AND CVE_EMPRESA     = ${pCveEmpresa}
		                AND F_FOTO          = ${vlBufPreMovto.F_OPERACION}
		                AND ID_CUENTA       = ${vlBufPreMovto.ID_CUENTA}
		                AND CVE_DIVISA      = ${vlBufPreMovto.CVE_DIVISA}"""
		    }else{
			//NO EXISTE EL SALDO PARA LA CUENTA
		        //INSERTA EL SALDO DE LA CUENTA CON LA FOTO
		        sql.execute("""
					INSERT  INTO PFIN_SALDO
		                        VALUES (${pCveGpoEmpresa},
						${pCveEmpresa},
						${vlBufPreMovto.F_OPERACION},
						${vlBufPreMovto.ID_CUENTA},
						${vlBufPreMovto.CVE_DIVISA},
		                        	(${vlBufPreMovto.IMP_NETO} * ${afectaSaldo}))
				""")               
            	    }             
		}
		// En caso de éxito se regresa el id del movimiento que se generó
		
	}
	return vlIdMovimiento;
    }
}


