//PKG_PROCESOS
class PKG_PROCESOS {

    String toString(){"PKG_PROCESOS"}

    def dameFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql){
	    def row = sql.firstRow("""
		       SELECT  TO_CHAR(TO_DATE(F_MEDIO,'DD-MM-YYYY'),'DD-MON-YYYY')  F_MEDIO
			FROM    PFIN_PARAMETRO
			WHERE   CVE_GPO_EMPRESA = ${pCveGpoEmpresa}
			    AND CVE_EMPRESA     = ${pCveEmpresa}
			    AND CVE_MEDIO       = 'SYSTEM' """)
	    return row.F_MEDIO
    }
   
    def pGeneraPreMovto(
		pCveGpoEmpresa,
                pCveEmpresa,
                pIdPreMovto,
                pFLiquidacion,
                pIdCuenta,
                pIdPrestamo,
                pCveDivisa,
                pCveOperacion,
                pImpNeto,
                pCveMedio,
                pCveMercado,
                pTxNota,
                pIdGrupo,
                pCveUsuario,
                pFValor,
                pNumPagoAmort,
                pTxrespuesta,
		sql){

	def F_OPERACION = dameFechaSistema(pCveGpoEmpresa,pCveEmpresa,sql)

	// IF ELSE Que nunca se utiliza en el paquete  PKG_PROCESOS.pGeneraPreMovto
	// IF pFLiquidacion < V.F_OPERACION AND pCveMercado <> 'PRESTAMO' THEN

        def CVE_GPO_EMPRESA        = pCveGpoEmpresa
        def CVE_EMPRESA            = pCveEmpresa
        def F_LIQUIDACION          = pFLiquidacion
        def ID_PRE_MOVIMIENTO      = pIdPreMovto
        def ID_CUENTA              = pIdCuenta
        def ID_PRESTAMO            = pIdPrestamo
        def CVE_DIVISA             = pCveDivisa
        def CVE_OPERACION          = pCveOperacion
        def IMP_NETO               = pImpNeto
        def PREC_OPERACION         = 0
        def TIPO_CAMBIO            = 0
        def CVE_MERCADO            = pCveMercado
        def CVE_MEDIO              = pCveMedio
        def TX_NOTA                = pTxNota
        def ID_GRUPO               = pIdGrupo
        def ID_MOVIMIENTO          = 0
        def CVE_USUARIO            = pCveUsuario
        def SIT_PRE_MOVIMIENTO     = 'NP'
        def F_APLICACION           = pFValor
        def NUM_PAGO_AMORTIZACION  = pNumPagoAmort

 	sql.execute("""
		Insert into PFIN_PRE_MOVIMIENTO (
			F_OPERACION,
			CVE_GPO_EMPRESA,
			CVE_EMPRESA,
			F_LIQUIDACION,
			ID_PRE_MOVIMIENTO,
			ID_CUENTA,
			ID_PRESTAMO,
			CVE_DIVISA,
			CVE_OPERACION,
			IMP_NETO,
			PREC_OPERACION,
			TIPO_CAMBIO,
			CVE_MERCADO,
			CVE_MEDIO,
			TX_NOTA,
			ID_GRUPO,
			ID_MOVIMIENTO,
			CVE_USUARIO,
			SIT_PRE_MOVIMIENTO,
			F_APLICACION,
			NUM_PAGO_AMORTIZACION) 
		values( 
			TO_DATE(${F_OPERACION},'DD-MM-YYYY'),
			${CVE_GPO_EMPRESA},
			${CVE_EMPRESA},
			TO_DATE(${F_LIQUIDACION},'DD-MM-YYYY'),
			${ID_PRE_MOVIMIENTO},
			${ID_CUENTA},
			${ID_PRESTAMO},
			${CVE_DIVISA},
			${CVE_OPERACION},
			${IMP_NETO},
			${PREC_OPERACION},
			${TIPO_CAMBIO},
			${CVE_MERCADO},
			${CVE_MEDIO},
			${TX_NOTA},
			${ID_GRUPO},
			${ID_MOVIMIENTO},
			${CVE_USUARIO},
			${SIT_PRE_MOVIMIENTO},
			TO_DATE(${F_APLICACION},'DD-MM-YYYY'),
			${NUM_PAGO_AMORTIZACION})
		""")
    }

    def pGeneraPreMovtoDet(
	pCveGpoEmpresa,
	pCveEmpresa,
	pIdPreMovto,
	pCveConcepto,
	pImpNeto,
	pTxNota,
	pTxrespuesta,
	sql
	){
	        //Se asignan los valores a las variables
		def CVE_GPO_EMPRESA   = pCveGpoEmpresa
		def CVE_EMPRESA       = pCveEmpresa
		def ID_PRE_MOVIMIENTO = pIdPreMovto
		def CVE_CONCEPTO      = pCveConcepto
		def IMP_CONCEPTO      = pImpNeto
		def TX_NOTA           = pTxNota

	 	sql.execute("""
			Insert into PFIN_PRE_MOVIMIENTO_DET (
				CVE_GPO_EMPRESA,
				CVE_EMPRESA,
				ID_PRE_MOVIMIENTO,
				CVE_CONCEPTO,
				IMP_CONCEPTO,
				TX_NOTA)
			values(
				${CVE_GPO_EMPRESA},
				${CVE_EMPRESA},
				${ID_PRE_MOVIMIENTO},
				${CVE_CONCEPTO},
				${IMP_CONCEPTO},
				${TX_NOTA})
		""")

	}
}


