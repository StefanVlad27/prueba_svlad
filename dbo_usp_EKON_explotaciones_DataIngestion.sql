USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[usp_EKON_explotaciones_DataIngestion]    Script Date: 20/03/2023 16:33:30 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_EKON_explotaciones_DataIngestion]
-- =============================================
-- Author:		Marcos Ayuso
-- Create date: 06/03/2023
-- Description:	Procedimiento almacenado para la carga de la tabla [CINCAPORC_DW].[EKON].[explotaciones] desde EKON
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
			@Aud_FechaHora_Actual datetime,
			@Aud_FechaHora_Actual_String varchar(30)
	SET @Aud_Tabla = 'explotaciones'
	SET @Aud_Esquema = 'EKON'
	SET @Aud_Fecha_Inicio = getdate()
	SET @Aud_Usuario = SYSTEM_USER
	SET @Aud_Hostname = HOST_NAME()

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
				[Explotacion] int,
				[Nombre] varchar (250),
				[Latitud] varchar (30),
				[Longitud] varchar (30),
				[Tipo] varchar (10),
				[DesTipo] varchar (50),
				[Rega] varchar (25),
				[Capacidad] int,
				[CodVisitador] int,
				[Visitador] varchar (50),
				[CodVeterinario] varchar (5),
				[Veterinario] varchar (100),
				[Fecha_creacion] datetime,
				[Usuario_creacion] varchar(40),
				[Hostname_creacion] varchar(35),
				[Fecha_modificacion] datetime,
				[Usuario_modificacion] varchar(40),
				[Hostname_modificacion] varchar(35),
				[Flag_borrado] bit);'
		EXEC sp_executesql @Aud_Query

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
		SET @Aud_Query = N'SELECT * INTO ' + @tabla_temporal + ' FROM OPENQUERY([' + @Servidor_origen + '], ''SELECT * FROM ' + @BBDD_origen + '.[IMP].[bi_explotaciones]' + ''');' --sin matadero
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'SELECT 
		                 [Explotacion],
						 [Nombre] COLLATE Modern_Spanish_CI_AS [Nombre],
						 [Latitud] COLLATE Modern_Spanish_CI_AS [Latitud],
						 [Longitud] COLLATE Modern_Spanish_CI_AS [Longitud],
						 [Tipo] COLLATE Modern_Spanish_CI_AS [Tipo],
						 [DesTipo] COLLATE Modern_Spanish_CI_AS [DesTipo],
						 [Rega] COLLATE Modern_Spanish_CI_AS [Rega],
						 [Capacidad],
						 [CodVisitador],
						 [Visitador] COLLATE Modern_Spanish_CI_AS [Visitador],
						 [CodVeterinario] COLLATE Modern_Spanish_CI_AS [CodVeterinario],
						 [Veterinario] COLLATE Modern_Spanish_CI_AS [Veterinario]
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
		EXEC('MERGE [CINCAPORC_DW].[EKON].[explotaciones] AS TARGET
		USING ' + @tabla_temporal_final + ' AS SOURCE
		ON (TARGET.[Explotacion] = SOURCE.[Explotacion]) 
		
		WHEN NOT MATCHED BY TARGET
			THEN INSERT ([Explotacion],
				[Nombre],
				[Latitud],
				[Longitud],
				[Tipo],
				[DesTipo],
				[Rega],
				[Capacidad],
				[CodVisitador],
				[Visitador],
				[CodVeterinario],
				[Veterinario],
				[Fecha_creacion],
				[Usuario_creacion],
				[Hostname_creacion],
				[Fecha_modificacion],
				[Usuario_modificacion],
				[Hostname_modificacion],
				[Flag_borrado])
			VALUES (
					SOURCE.[Explotacion]
					,SOURCE.[Nombre]
					,SOURCE.[Latitud]
					,SOURCE.[Longitud]
					,SOURCE.[Tipo]
					,SOURCE.[DesTipo]
					,SOURCE.[Rega]
					,SOURCE.[Capacidad]
					,SOURCE.[CodVisitador]
					,SOURCE.[Visitador]
					,SOURCE.[CodVeterinario]
					,SOURCE.[Veterinario]
					,''' + @Aud_FechaHora_Actual_String + '''
					,''' + @Aud_Usuario + '''
					,''' + @Aud_Hostname + '''
					,NULL
					,NULL
					,NULL
					,NULL)
		
		WHEN MATCHED AND ISNULL(NULLIF(TARGET.[Nombre], SOURCE.[Nombre]), NULLIF(SOURCE.[Nombre], TARGET.[Nombre])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Latitud], SOURCE.[Latitud]), NULLIF(SOURCE.[Latitud], TARGET.[Latitud])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Longitud], SOURCE.[Longitud]), NULLIF(SOURCE.[Longitud], TARGET.[Longitud])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Tipo], SOURCE.[Tipo]), NULLIF(SOURCE.[Tipo], TARGET.[Tipo])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[DesTipo], SOURCE.[DesTipo]), NULLIF(SOURCE.[DesTipo], TARGET.[DesTipo])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Rega], SOURCE.[Rega]), NULLIF(SOURCE.[Rega], TARGET.[Rega])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Capacidad], SOURCE.[Capacidad]), NULLIF(SOURCE.[Capacidad], TARGET.[Capacidad])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[CodVisitador], SOURCE.[CodVisitador]), NULLIF(SOURCE.[CodVisitador], TARGET.[CodVisitador])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Visitador], SOURCE.[Visitador]), NULLIF(SOURCE.[Visitador], TARGET.[Visitador])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[CodVeterinario], SOURCE.[CodVeterinario]), NULLIF(SOURCE.[CodVeterinario], TARGET.[CodVeterinario])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[Veterinario], SOURCE.[Veterinario]), NULLIF(SOURCE.[Veterinario], TARGET.[Veterinario])) IS NOT NULL
							OR TARGET.[Flag_borrado] IS NOT NULL
		THEN UPDATE SET TARGET.[Nombre] = SOURCE.[Nombre]
						,TARGET.[Latitud] = SOURCE.[Latitud]
						,TARGET.[Longitud] = SOURCE.[Longitud]
						,TARGET.[Tipo] = SOURCE.[Tipo]
						,TARGET.[DesTipo] = SOURCE.[DesTipo]
						,TARGET.[Rega] = SOURCE.[Rega]
						,TARGET.[Capacidad] = SOURCE.[Capacidad]
						,TARGET.[CodVisitador] = SOURCE.[CodVisitador]
						,TARGET.[Visitador] = SOURCE.[Visitador]
						,TARGET.[CodVeterinario] = SOURCE.[CodVeterinario]
						,TARGET.[Veterinario] = SOURCE.[Veterinario]
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
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Nombre] ELSE DELETED.[Nombre] END AS [Nombre],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Latitud] ELSE DELETED.[Latitud] END AS [Latitud],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Longitud] ELSE DELETED.[Longitud] END AS [Longitud],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Tipo] ELSE DELETED.[Tipo] END AS [Tipo],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[DesTipo] ELSE DELETED.[DesTipo] END AS [DesTipo],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Rega] ELSE DELETED.[Rega] END AS [Rega],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Capacidad] ELSE DELETED.[Capacidad] END AS [Capacidad],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[CodVisitador] ELSE DELETED.[CodVisitador] END AS [CodVisitador],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Visitador] ELSE DELETED.[Visitador] END AS [Visitador],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[CodVeterinario] ELSE DELETED.[CodVeterinario] END AS [CodVeterinario],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[Veterinario] ELSE DELETED.[Veterinario] END AS [Veterinario],
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

		--Carga completa, no se realiza el cuadre de datos
		--EXEC dbo.usp_Volumetry_Validation @Aud_Tabla, @Aud_Esquema, @Servidor_origen, @BBDD_origen, 'FECHA', 'SQL Server'

	END TRY
	BEGIN CATCH

		SELECT @ErrorNumber = CAST(ERROR_NUMBER() AS varchar), @ErrorState = CAST(ERROR_STATE() AS varchar), @ErrorSeverity = CAST(ERROR_SEVERITY() AS varchar), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorLine = CAST(ERROR_LINE() AS varchar), @ErrorMessage = REPLACE(ERROR_MESSAGE(),'''','"');
		
		--Mensaje de auditoría
		EXEC dbo.usp_OutputErrorMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 3, @ErrorNumber, @ErrorState, @ErrorSeverity, @ErrorProcedure, @ErrorLine, @ErrorMessage

		RETURN -1
		
	END CATCH;

END
GO