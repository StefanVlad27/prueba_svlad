USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[usp_EKON_movimientos_DataIngestion]    Script Date: 20/03/2023 16:38:52 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- !! OJO identificador no es PK!

CREATE PROCEDURE [dbo].[usp_EKON_movimientos_DataIngestion]
-- =============================================
-- Author:		Ignacio Morer
-- Create date: 10/03/2023
-- Description:	Procedimiento almacenado para la carga de la tabla [CINCAPORC_DW].[EKON].[movimientos] desde EKON
-- =============================================
	--Definición parámetros
	@Fecha_carga_f date = NULL

AS
BEGIN
	SET NOCOUNT ON
	--Inicialización de variables de auditoría
	DECLARE @Aud_Tabla varchar(250),
			@Aud_Esquema varchar(5),
			@Aud_Fecha_Inicio datetime,
			@Aud_Usuario varchar(50),
			@Aud_Hostname varchar(50),
			--@Aud_Net varchar(50),
			--@Aud_Client_Net varchar(50),
			@Aud_FechaHora_Actual datetime,
			@Aud_FechaHora_Actual_String varchar(30)
	SET @Aud_Tabla = 'movimientos'
	SET @Aud_Esquema = 'EKON'
	SET @Aud_Fecha_Inicio = getdate()
	SET @Aud_Usuario = SYSTEM_USER
	SET @Aud_Hostname = HOST_NAME()
	--SELECT @Aud_Net = net_address, @Aud_Client_Net = client_net_address FROM sys.sysprocesses AS S INNER JOIN sys.dm_exec_connections AS decc ON S.spid = decc.session_id WHERE spid = @@SPID;

	--Inicialización del mensaje de auditoría
	DECLARE @Aud_Query nvarchar(MAX),
    @Aud_Asunto varchar(600)

	--Mensaje de auditoría
	EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 1	

	--Inicialización de variables y parámetros no obligatorios
	--En caso de no pasar fecha, se tomará como fecha de referencia el día actual
	SET @Fecha_carga_f = ISNULL(@Fecha_carga_f,getdate())
	DECLARE @String_fecha_f varchar(10)
	SET @String_fecha_f = CONVERT(varchar(10), @Fecha_carga_f, 23)

	DECLARE @Fecha_carga_i date
	SET @Fecha_carga_i = DATEADD(week, -1, @Fecha_carga_f)
	DECLARE @String_fecha_i varchar(10)
	SET @String_fecha_i = CONVERT(varchar(10), @Fecha_carga_i, 23)

	DECLARE @Fecha_tabla varchar(8)
	SET @Fecha_tabla = CONVERT(varchar(8), @Fecha_carga_f, 112)
	
	DECLARE @tabla_temporal varchar(100),
	@tabla_temporal_final varchar (100)
	SET @tabla_temporal = CONCAT('[CINCAPORC_DW].[TMP].[',@Aud_Tabla,'_',@Fecha_tabla,']')
	SET @tabla_temporal_final = CONCAT('[CINCAPORC_DW].[TMP].[',@Aud_Tabla,'_',@Fecha_tabla,'_final]')
	
	DECLARE @Servidor_origen varchar(20),
			@BBDD_origen varchar(30)
	SET @Servidor_origen = '172.31.10.217'
	SET @BBDD_origen = 'N065OBKR' 
	
	DECLARE @ErrorNumber varchar(15),
	@ErrorState varchar(15),
	@ErrorSeverity varchar(15),
	@ErrorProcedure varchar(50),
	@ErrorLine varchar(15),
	@ErrorMessage varchar(300)
	
	--Creación de tabla de auditoría de integración
	DECLARE @tabla_auditoria_integracion varchar(100),
			@tabla_auditoria_integracion_f varchar(60),
			@tabla_auditoria_integracion_b varchar(60)
	SET @tabla_auditoria_integracion_b = CONCAT('tbl_Auditoria_Integracion_',@Aud_Tabla,'_%') 
	SET @tabla_auditoria_integracion_f = CONCAT('tbl_Auditoria_Integracion_',@Aud_Tabla,'_',@Fecha_tabla) 
	SET @tabla_auditoria_integracion = CONCAT('[CINCAPORC_DW].[TMP].[tbl_Auditoria_Integracion_',@Aud_Tabla,'_',@Fecha_tabla,']') 

	--Mensaje de auditoría
	EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 2

	BEGIN TRY

		SET @Aud_Query = N'DROP TABLE IF EXISTS ' + @tabla_auditoria_integracion
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'CREATE TABLE ' + @tabla_auditoria_integracion + ' (Accion varchar(10) NOT NULL,
						[proveedor_origen] [varchar](1),
						[ex_origen] [int],
						[rega_origen] [varchar](35),
						[lote_origen] [varchar](15),
						[ex_destino] [int],
						[rega_destino] [varchar](35),
						[lote_destino] [varchar](15),
						[fecha] [datetime],
						[transportista] [varchar](20),
						[matricula] [varchar](10),
						[unid] [decimal](38,4), 
						[kilos] [decimal](38,4),
						[importe] [decimal](38,4), 
						[articulo] [varchar](25),
						[descripcion] [varchar](40),
						[identificador] [varchar](61),
						[tipo] [varchar](3),
						[Fecha_creacion] [datetime],
						[Usuario_creacion] [varchar](40),
						[Hostname_creacion] [varchar](35),
						[Fecha_modificacion] [datetime],
						[Usuario_modificacion] [varchar](40),
						[Hostname_modificacion] [varchar](35),
						[Flag_borrado] [bit]);'
		EXEC sp_executesql @Aud_Query
		-- quitado de la query anterior
		--[Net_creacion] varchar(15),
		--[Client_Net_creacion] varchar(15),

		--Mensaje de auditoría
		EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 3

	END TRY
	BEGIN CATCH

		SELECT @ErrorNumber = CAST(ERROR_NUMBER() AS varchar), @ErrorState = CAST(ERROR_STATE() AS varchar), @ErrorSeverity = CAST(ERROR_SEVERITY() AS varchar), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorLine = CAST(ERROR_LINE() AS varchar), @ErrorMessage = REPLACE(ERROR_MESSAGE(),'''','"');
	
		--Mensaje de auditoría
		EXEC dbo.usp_OutputErrorMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 1, @ErrorNumber, @ErrorState, @ErrorSeverity, @ErrorProcedure, @ErrorLine, @ErrorMessage

		RETURN -1
		
	END CATCH;

	--Lectura de datos de origen
	SET @Aud_Query = N'DROP TABLE IF EXISTS ' + @tabla_temporal
	EXEC sp_executesql @Aud_Query

	--Mensaje de auditoría
	EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 4

	--Comprobación de la conexión con el servidor de origen
	BEGIN TRY
		SET @Aud_Query = N'SELECT * INTO ' + @tabla_temporal + ' FROM OPENQUERY([' + @Servidor_origen + '], ''SELECT * FROM ' + @BBDD_origen + '.[IMP].[bi_movimientos]' + ''');' 
		
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'SELECT 
						[proveedor_origen] COLLATE Modern_Spanish_CI_AS [proveedor_origen],
						[ex_origen],
						[rega_origen] COLLATE Modern_Spanish_CI_AS [rega_origen],
						[lote_origen] COLLATE Modern_Spanish_CI_AS [lote_origen],
						[ex_destino],
						[rega_destino] COLLATE Modern_Spanish_CI_AS [rega_destino],
						[lote_destino] COLLATE Modern_Spanish_CI_AS [lote_destino],
						[fecha],
						[transportista] COLLATE Modern_Spanish_CI_AS [transportista],
						[matricula] COLLATE Modern_Spanish_CI_AS [matricula],
						[unid], 
						[kilos],
						[importe], 
						[articulo] COLLATE Modern_Spanish_CI_AS [articulo],
						[descripcion] COLLATE Modern_Spanish_CI_AS [descripcion],
						[identificador] COLLATE Modern_Spanish_CI_AS [identificador],
						[tipo] COLLATE Modern_Spanish_CI_AS [tipo] 
						INTO ' + @tabla_temporal_final + '
						FROM ' + @tabla_temporal + ';'
		EXEC sp_executesql @Aud_Query
	
		--Mensaje de auditoría
		EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 5

	END TRY
	BEGIN CATCH

		SELECT @ErrorNumber = CAST(ERROR_NUMBER() AS varchar), @ErrorState = CAST(ERROR_STATE() AS varchar), @ErrorSeverity = CAST(ERROR_SEVERITY() AS varchar), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorLine = CAST(ERROR_LINE() AS varchar), @ErrorMessage = REPLACE(ERROR_MESSAGE(),'''','"');
		
		--Mensaje de auditoría
		EXEC dbo.usp_OutputErrorMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 2, @ErrorNumber, @ErrorState, @ErrorSeverity, @ErrorProcedure, @ErrorLine, @ErrorMessage
	
		RETURN -1
		
	END CATCH;
	
	--Mensaje de auditoría
	EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 6

	BEGIN TRY
	
		--Instante de tiempo en el que se realiza el merge
		SET @Aud_FechaHora_Actual = getdate()
		SET @Aud_FechaHora_Actual_String = CONVERT(varchar, @Aud_FechaHora_Actual, 121)

		--Integración de datos
		EXEC('MERGE [CINCAPORC_DW].[EKON].[movimientos] AS TARGET
		USING ' + @tabla_temporal_final + ' AS SOURCE
		ON (TARGET.[identificador] = SOURCE.[identificador]) 
		
		WHEN NOT MATCHED BY TARGET
			THEN INSERT ([proveedor_origen],
			[ex_origen],
			[rega_origen],
			[lote_origen],
			[ex_destino],
			[rega_destino],
			[lote_destino],
			[fecha],
			[transportista],
			[matricula],
			[unid], 
			[kilos],
			[importe], 
			[articulo],
			[descripcion],
			[identificador],
			[tipo],
			[Fecha_creacion],
			[Usuario_creacion],
			[Hostname_creacion],
			[Fecha_modificacion],
			[Usuario_modificacion],
			[Hostname_modificacion],
			[Flag_borrado])
			VALUES (
					 SOURCE.[proveedor_origen]
					,SOURCE.[ex_origen]
					,SOURCE.[rega_origen]
					,SOURCE.[lote_origen]
					,SOURCE.[ex_destino]
					,SOURCE.[rega_destino]
					,SOURCE.[lote_destino]
					,SOURCE.[fecha]
					,SOURCE.[transportista]
					,SOURCE.[matricula]
					,SOURCE.[unid]
					,SOURCE.[kilos]
					,SOURCE.[importe]
					,SOURCE.[articulo]
					,SOURCE.[descripcion]
					,SOURCE.[identificador]
					,SOURCE.[tipo]
					,''' + @Aud_FechaHora_Actual_String + '''
					,''' + @Aud_Usuario + '''
					,''' + @Aud_Hostname + '''
					,NULL
					,NULL
					,NULL
					,NULL)
		
		WHEN MATCHED AND ISNULL(NULLIF(TARGET.[proveedor_origen], SOURCE.[proveedor_origen]), NULLIF(SOURCE.[proveedor_origen], TARGET.[proveedor_origen])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[ex_origen], SOURCE.[ex_origen]), NULLIF(SOURCE.[ex_origen], TARGET.[ex_origen])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[rega_origen], SOURCE.[rega_origen]), NULLIF(SOURCE.[rega_origen], TARGET.[rega_origen])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[lote_origen], SOURCE.[lote_origen]), NULLIF(SOURCE.[lote_origen], TARGET.[lote_origen])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[ex_destino], SOURCE.[ex_destino]), NULLIF(SOURCE.[ex_destino], TARGET.[ex_destino])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[rega_destino], SOURCE.[rega_destino]), NULLIF(SOURCE.[rega_destino], TARGET.[rega_destino])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[lote_destino], SOURCE.[lote_destino]), NULLIF(SOURCE.[lote_destino], TARGET.[lote_destino])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[fecha], SOURCE.[fecha]), NULLIF(SOURCE.[fecha], TARGET.[fecha])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[transportista], SOURCE.[transportista]), NULLIF(SOURCE.[transportista], TARGET.[transportista])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[matricula], SOURCE.[matricula]), NULLIF(SOURCE.[matricula], TARGET.[matricula])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[unid], SOURCE.[unid]), NULLIF(SOURCE.[unid], TARGET.[unid])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[kilos], SOURCE.[kilos]), NULLIF(SOURCE.[kilos], TARGET.[kilos])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[importe], SOURCE.[importe]), NULLIF(SOURCE.[importe], TARGET.[importe])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[articulo], SOURCE.[articulo]), NULLIF(SOURCE.[articulo], TARGET.[articulo])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[descripcion], SOURCE.[descripcion]), NULLIF(SOURCE.[descripcion], TARGET.[descripcion])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[identificador], SOURCE.[identificador]), NULLIF(SOURCE.[identificador], TARGET.[identificador])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[tipo], SOURCE.[tipo]), NULLIF(SOURCE.[tipo], TARGET.[tipo])) IS NOT NULL							
							OR TARGET.[Flag_borrado] IS NOT NULL
		THEN UPDATE SET TARGET.[proveedor_origen] = SOURCE.[proveedor_origen]
						,TARGET.[ex_origen] = SOURCE.[ex_origen]
						,TARGET.[rega_origen] = SOURCE.[rega_origen]
						,TARGET.[lote_origen] = SOURCE.[lote_origen]
						,TARGET.[ex_destino] = SOURCE.[ex_destino]
						,TARGET.[rega_destino] = SOURCE.[rega_destino]
						,TARGET.[lote_destino] = SOURCE.[lote_destino]
						,TARGET.[fecha] = SOURCE.[fecha]
						,TARGET.[transportista] = SOURCE.[transportista]
						,TARGET.[matricula] = SOURCE.[matricula]
						,TARGET.[unid] = SOURCE.[unid]
						,TARGET.[kilos] = SOURCE.[kilos]
						,TARGET.[importe] = SOURCE.[importe]
						,TARGET.[articulo] = SOURCE.[articulo]
						,TARGET.[descripcion] = SOURCE.[descripcion]
						,TARGET.[identificador] = SOURCE.[identificador]
						,TARGET.[tipo] = SOURCE.[tipo]
						,TARGET.[Fecha_modificacion]= ''' + @Aud_FechaHora_Actual_String + '''
						,TARGET.[Usuario_modificacion]= ''' + @Aud_Usuario + '''
						,TARGET.[Hostname_modificacion]= ''' + @Aud_Hostname + '''
						,TARGET.[Flag_borrado]=NULL
		
		WHEN NOT MATCHED BY SOURCE AND TARGET.[Flag_borrado] IS NULL
			THEN UPDATE SET TARGET.[Fecha_modificacion]       = ''' + @Aud_FechaHora_Actual_String + '''
							,TARGET.[Usuario_modificacion]    = ''' + @Aud_Usuario + '''
							,TARGET.[Hostname_modificacion]   = ''' + @Aud_Hostname + '''
							,TARGET.[Flag_borrado]=1
	
		--Salida del cruce para auditoría
		OUTPUT $action,
			CASE WHEN $action=''INSERT'' THEN INSERTED.[proveedor_origen] ELSE DELETED.[proveedor_origen] END AS [proveedor_origen],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[ex_origen] ELSE DELETED.[ex_origen] END AS [ex_origen],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[rega_origen] ELSE DELETED.[rega_origen] END AS [rega_origen],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[lote_origen] ELSE DELETED.[lote_origen] END AS [lote_origen],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[ex_destino] ELSE DELETED.[ex_destino] END AS [ex_destino],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[rega_destino] ELSE DELETED.[rega_destino] END AS [rega_destino],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[lote_destino] ELSE DELETED.[lote_destino] END AS [lote_destino],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[fecha] ELSE DELETED.[fecha] END AS [fecha],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[transportista] ELSE DELETED.[transportista] END AS [transportista],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[matricula] ELSE DELETED.[matricula] END AS [matricula],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[unid] ELSE DELETED.[unid] END AS [unid],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[kilos] ELSE DELETED.[kilos] END AS [kilos],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[importe] ELSE DELETED.[importe] END AS [importe],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[articulo] ELSE DELETED.[articulo] END AS [articulo],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[descripcion] ELSE DELETED.[descripcion] END AS [descripcion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[identificador] ELSE DELETED.[identificador] END AS [identificador],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[tipo] ELSE DELETED.[tipo] END AS [tipo],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Fecha_creacion] ELSE DELETED.[Fecha_creacion] END AS [Fecha_creacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Usuario_creacion] ELSE DELETED.[Usuario_creacion] END AS [Usuario_creacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Hostname_creacion] ELSE DELETED.[Hostname_creacion] END AS [Hostname_creacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Fecha_modificacion] ELSE DELETED.[Fecha_modificacion] END AS [Fecha_modificacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Usuario_modificacion] ELSE DELETED.[Usuario_modificacion] END AS [Usuario_modificacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Hostname_modificacion] ELSE DELETED.[Hostname_modificacion] END AS [Hostname_modificacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Flag_borrado] ELSE DELETED.[Flag_borrado] END AS [Flag_borrado]
		INTO ' + @tabla_auditoria_integracion + ';')

		--Control registros modificados
		DECLARE @Consulta_insertados varchar(200),
			@Consulta_actualizados varchar(200),
			@Consulta_borrados varchar(200),
			@Registros_insertados varchar(15),
			@Registros_actualizados varchar(15),
			@Registros_borrados varchar(15)

		SET @Aud_Query = N'SELECT @Registros_insertados = COUNT(*) FROM ' + @tabla_auditoria_integracion + ' WHERE Accion = ''INSERT'';'
		EXEC sp_executesql @Aud_Query, N'@Registros_insertados INT OUTPUT', @Registros_insertados = @Registros_insertados OUTPUT
		SET @Aud_Query = N'SELECT @Registros_actualizados = COUNT(*) FROM ' + @tabla_auditoria_integracion + ' WHERE Accion = ''UPDATE'' AND Flag_borrado IS NULL;'
		EXEC sp_executesql @Aud_Query, N'@Registros_actualizados INT OUTPUT', @Registros_actualizados = @Registros_actualizados OUTPUT
		SET @Aud_Query = N'SELECT @Registros_borrados = COUNT(*) FROM ' + @tabla_auditoria_integracion + ' WHERE Accion = ''UPDATE'' AND Flag_borrado IS NOT NULL;'
		EXEC sp_executesql @Aud_Query, N'@Registros_borrados INT OUTPUT', @Registros_borrados = @Registros_borrados OUTPUT
		
		SET @Aud_Query = N'DROP TABLE ' + @tabla_temporal
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'DROP TABLE ' + @tabla_temporal_final
		EXEC sp_executesql @Aud_Query
	
		--Se respetan la tabla temporal de auditoría generada por la ejecución actual y la que tiene la fecha más reciente
		SET @Aud_Query = N'SELECT (''DROP TABLE ['' + TABLE_CATALOG + ''].['' + TABLE_SCHEMA + ''].['' + TABLE_NAME + ''];'') Collate Modern_Spanish_CI_AS AS Tablas_borrar
				INTO [TMP].[Tablas_auditoria_borrar_e]
					FROM INFORMATION_SCHEMA.TABLES
					WHERE TABLE_CATALOG = ''CINCAPORC_DW''
						AND TABLE_SCHEMA = ''TMP''
						AND TABLE_NAME LIKE (''' + @tabla_auditoria_integracion_b + ''')
						AND RIGHT(TABLE_NAME, 8)<>(SELECT MAX(RIGHT(TABLE_NAME, 8))
													FROM INFORMATION_SCHEMA.TABLES
													WHERE TABLE_CATALOG = ''CINCAPORC_DW''
														AND TABLE_SCHEMA = ''TMP''
														AND TABLE_NAME LIKE (''' + @tabla_auditoria_integracion_b + '''))
						AND TABLE_NAME<>''' + @tabla_auditoria_integracion_f + ''';'
		EXEC sp_executesql @Aud_Query		
	
		DECLARE @bucle varchar(4000)
		DECLARE bucles CURSOR FOR
			SELECT Tablas_borrar
			FROM [TMP].[Tablas_auditoria_borrar_e]
			
			OPEN bucles
			WHILE 1 = 1
			BEGIN
				FETCH bucles INTO @bucle
				IF @@fetch_status != 0 BREAK
				EXEC(@bucle)
			END
		CLOSE bucles;
		DEALLOCATE bucles
	
		EXEC sp_executesql N'DROP TABLE [TMP].[Tablas_auditoria_borrar_e];'
		
		--Mensaje de auditoría
		EXEC dbo.usp_OutputEndMessage @Aud_Tabla, @Aud_Esquema, @Fecha_carga_f, @Aud_Fecha_Inicio, @Registros_insertados, @Registros_actualizados, @Registros_borrados

--		Carga completa, no se realiza el cuadre de datos
--		EXEC dbo.usp_Volumetry_Validation @Aud_Tabla, @Aud_Esquema, @Servidor_origen, @BBDD_origen, 'FECHA'

	END TRY
	BEGIN CATCH

		SELECT @ErrorNumber = CAST(ERROR_NUMBER() AS varchar), @ErrorState = CAST(ERROR_STATE() AS varchar), @ErrorSeverity = CAST(ERROR_SEVERITY() AS varchar), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorLine = CAST(ERROR_LINE() AS varchar), @ErrorMessage = REPLACE(ERROR_MESSAGE(),'''','"');
		
		--Mensaje de auditoría
		EXEC dbo.usp_OutputErrorMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 3, @ErrorNumber, @ErrorState, @ErrorSeverity, @ErrorProcedure, @ErrorLine, @ErrorMessage

		RETURN -1
		
	END CATCH;

END
GO