USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[usp_EKON_lotes_cerrados_DataIngestion]    Script Date: 20/03/2023 16:36:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_EKON_lotes_cerrados_DataIngestion]
-- =============================================
-- Author:		Ignacio Morer
-- Create date: 10/03/2023
-- Description:	Procedimiento almacenado para la carga de la tabla [CINCAPORC_DW].[EKON].[lotes_cerrados] desde EKON
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
	SET @Aud_Tabla = 'lotes_cerrados'
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
	SET @tabla_auditoria_integracion_b = CONCAT('tbl_Auditoria_Integracion_',@Aud_Tabla,'_%') --sin matadero
	SET @tabla_auditoria_integracion_f = CONCAT('tbl_Auditoria_Integracion_',@Aud_Tabla,'_',@Fecha_tabla) --sin matadero
	SET @tabla_auditoria_integracion = CONCAT('[CINCAPORC_DW].[TMP].[tbl_Auditoria_Integracion_',@Aud_Tabla,'_',@Fecha_tabla,']') --sin matadero

	--Mensaje de auditoría
	EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 2

	BEGIN TRY

		SET @Aud_Query = N'DROP TABLE IF EXISTS ' + @tabla_auditoria_integracion
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'CREATE TABLE ' + @tabla_auditoria_integracion + ' (Accion varchar(10) NOT NULL,
						[Explotacion] [smallint],
						[Rega] [varchar](25),
						[Lote] [varchar](7),
						[fecha_apertura] [datetime],
						[fecha_fin] [datetime],
						[origen] [varchar](40),
						[cantE] [decimal](38,4),
						[kilosE] [decimal](38,4),
						[importeE] [decimal](38,4),
						[cantS] [decimal](38,4),
						[kilosS] [decimal](38,4),
						[importeS] [decimal](38,4),
						[bajas] [decimal](38,4),
						[kilosP] [decimal](38,4),
						[importeP] [decimal](38,4),
						[costeP] [int],
						[transporte] [decimal](38,4),
						[medicamentos] [decimal](38,4),
						[varios] [decimal](38,4),
						[otros] [decimal](38,4),
						[importegr] [decimal](38,4),
						[WTF] [int],
						[Fecha_creacion] datetime,
						[Usuario_creacion] varchar(40),
						[Hostname_creacion] varchar(35),
						[Fecha_modificacion] datetime,
						[Usuario_modificacion] varchar(40),
						[Hostname_modificacion] varchar(35),
						[Flag_borrado] bit);'
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
		SET @Aud_Query = N'SELECT * INTO ' + @tabla_temporal + ' FROM OPENQUERY([' + @Servidor_origen + '], ''SELECT * FROM ' + @BBDD_origen + '.[IMP].[bi_lotes_cerrados]' + ''');' 
		
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'SELECT 
						[Explotacion],
				 		[Rega] COLLATE Modern_Spanish_CI_AS [Rega],
						[Lote] COLLATE Modern_Spanish_CI_AS [Lote],
						[fecha_apertura],
						[fecha_fin],
						[origen] COLLATE Modern_Spanish_CI_AS [origen],
						[cantE],
						[kilosE],
						[importeE],
						[cantS],
						[kilosS],
						[importeS],
						[bajas],
						[kilosP],
						[importeP],
						[costeP],
						[transporte],
						[medicamentos],
						[varios],
						[otros],
						[importegr],
						[WTF] 
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
		EXEC('MERGE [CINCAPORC_DW].[EKON].[lotes_cerrados] AS TARGET
		USING ' + @tabla_temporal_final + ' AS SOURCE
		ON (TARGET.[Lote] = SOURCE.[Lote]) 
		
		WHEN NOT MATCHED BY TARGET
			THEN INSERT ([Explotacion],
				 		[Rega],
						[Lote],
						[fecha_apertura],
						[fecha_fin],
						[origen],
						[cantE],
						[kilosE],
						[importeE],
						[cantS],
						[kilosS],
						[importeS],
						[bajas],
						[kilosP],
						[importeP],
						[costeP],
						[transporte],
						[medicamentos],
						[varios],
						[otros],
						[importegr],
						[WTF], 
				[Fecha_creacion],
				[Usuario_creacion],
				[Hostname_creacion],
				[Fecha_modificacion],
				[Usuario_modificacion],
				[Hostname_modificacion],
				[Flag_borrado])
			VALUES (
					SOURCE.[Explotacion]
					,SOURCE.[Rega]
					,SOURCE.[Lote]
					,SOURCE.[fecha_apertura]
					,SOURCE.[fecha_fin]
					,SOURCE.[origen]
					,SOURCE.[cantE]
					,SOURCE.[kilosE]
					,SOURCE.[importeE]
					,SOURCE.[cantS]
					,SOURCE.[kilosS]
					,SOURCE.[importeS]
					,SOURCE.[bajas]
					,SOURCE.[kilosP]
					,SOURCE.[importeP]
					,SOURCE.[costeP]
					,SOURCE.[transporte]
					,SOURCE.[medicamentos]
					,SOURCE.[varios]
					,SOURCE.[otros]
					,SOURCE.[importegr]
					,SOURCE.[WTF]
					,''' + @Aud_FechaHora_Actual_String + '''
					,''' + @Aud_Usuario + '''
					,''' + @Aud_Hostname + '''
					,NULL
					,NULL
					,NULL
					,NULL)
		
		WHEN MATCHED AND ISNULL(NULLIF(TARGET.[Explotacion], SOURCE.[Explotacion]), NULLIF(SOURCE.[Explotacion], TARGET.[Explotacion])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Rega], SOURCE.[Rega]), NULLIF(SOURCE.[Rega], TARGET.[Rega])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Lote], SOURCE.[Lote]), NULLIF(SOURCE.[Lote], TARGET.[Lote])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[fecha_apertura], SOURCE.[fecha_apertura]), NULLIF(SOURCE.[fecha_apertura], TARGET.[fecha_apertura])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[fecha_fin], SOURCE.[fecha_fin]), NULLIF(SOURCE.[fecha_fin], TARGET.[fecha_fin])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[origen], SOURCE.[origen]), NULLIF(SOURCE.[origen], TARGET.[origen])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[cantE], SOURCE.[cantE]), NULLIF(SOURCE.[cantE], TARGET.[cantE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[kilosE], SOURCE.[kilosE]), NULLIF(SOURCE.[kilosE], TARGET.[kilosE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[importeE], SOURCE.[importeE]), NULLIF(SOURCE.[importeE], TARGET.[importeE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[cantS], SOURCE.[cantS]), NULLIF(SOURCE.[cantS], TARGET.[cantS])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[kilosS], SOURCE.[kilosS]), NULLIF(SOURCE.[kilosS], TARGET.[kilosS])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[importeS], SOURCE.[importeS]), NULLIF(SOURCE.[importeS], TARGET.[importeS])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[bajas], SOURCE.[bajas]), NULLIF(SOURCE.[bajas], TARGET.[bajas])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[kilosP], SOURCE.[kilosP]), NULLIF(SOURCE.[kilosP], TARGET.[kilosP])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[importeP], SOURCE.[importeP]), NULLIF(SOURCE.[importeP], TARGET.[importeP])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[costeP], SOURCE.[costeP]), NULLIF(SOURCE.[costeP], TARGET.[costeP])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[transporte], SOURCE.[transporte]), NULLIF(SOURCE.[transporte], TARGET.[transporte])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[medicamentos], SOURCE.[medicamentos]), NULLIF(SOURCE.[medicamentos], TARGET.[medicamentos])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[varios], SOURCE.[varios]), NULLIF(SOURCE.[varios], TARGET.[varios])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[otros], SOURCE.[otros]), NULLIF(SOURCE.[otros], TARGET.[otros])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[importegr], SOURCE.[importegr]), NULLIF(SOURCE.[importegr], TARGET.[importegr])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[WTF], SOURCE.[WTF]), NULLIF(SOURCE.[WTF], TARGET.[WTF])) IS NOT NULL
							OR TARGET.[Flag_borrado] IS NOT NULL
		THEN UPDATE SET TARGET.[Explotacion] = SOURCE.[Explotacion]
						,TARGET.[Rega] = SOURCE.[Rega]
						,TARGET.[Lote] = SOURCE.[Lote]
						,TARGET.[fecha_apertura] = SOURCE.[fecha_apertura]
						,TARGET.[fecha_fin] = SOURCE.[fecha_fin]
						,TARGET.[origen] = SOURCE.[origen]
						,TARGET.[cantE] = SOURCE.[cantE]
						,TARGET.[kilosE] = SOURCE.[kilosE]
						,TARGET.[importeE] = SOURCE.[importeE]
						,TARGET.[cantS] = SOURCE.[cantS]
						,TARGET.[kilosS] = SOURCE.[kilosS]
						,TARGET.[importeS] = SOURCE.[importeS]
						,TARGET.[bajas] = SOURCE.[bajas]
						,TARGET.[kilosP] = SOURCE.[kilosP]
						,TARGET.[importeP] = SOURCE.[importeP]
						,TARGET.[costeP] = SOURCE.[costeP]
						,TARGET.[transporte] = SOURCE.[transporte]
						,TARGET.[medicamentos] = SOURCE.[medicamentos]
						,TARGET.[varios] = SOURCE.[varios]
						,TARGET.[otros] = SOURCE.[otros]
						,TARGET.[importegr] = SOURCE.[importegr]
						,TARGET.[WTF] = SOURCE.[WTF]
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
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Explotacion] ELSE DELETED.[Explotacion] END AS [Explotacion],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Rega] ELSE DELETED.[Rega] END AS [Rega],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Lote] ELSE DELETED.[Lote] END AS [Lote],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[fecha_apertura] ELSE DELETED.[fecha_apertura] END AS [fecha_apertura],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[fecha_fin] ELSE DELETED.[fecha_fin] END AS [fecha_fin],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[origen] ELSE DELETED.[origen] END AS [origen],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[cantE] ELSE DELETED.[cantE] END AS [cantE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[kilosE] ELSE DELETED.[kilosE] END AS [kilosE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[importeE] ELSE DELETED.[importeE] END AS [importeE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[cantS] ELSE DELETED.[cantS] END AS [cantS],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[kilosS] ELSE DELETED.[kilosS] END AS [kilosS],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[importeS] ELSE DELETED.[importeS] END AS [importeS],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[bajas] ELSE DELETED.[bajas] END AS [bajas],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[kilosP] ELSE DELETED.[kilosP] END AS [kilosP],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[importeP] ELSE DELETED.[importeP] END AS [importeP],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[costeP] ELSE DELETED.[costeP] END AS [costeP],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[transporte] ELSE DELETED.[transporte] END AS [transporte],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[medicamentos] ELSE DELETED.[medicamentos] END AS [medicamentos],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[varios] ELSE DELETED.[varios] END AS [varios],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[otros] ELSE DELETED.[otros] END AS [otros],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[importegr] ELSE DELETED.[importegr] END AS [importegr],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[WTF] ELSE DELETED.[WTF] END AS [WTF],
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