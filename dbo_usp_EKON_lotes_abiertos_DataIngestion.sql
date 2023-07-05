USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[usp_EKON_lotes_abiertos_DataIngestion]    Script Date: 20/03/2023 16:35:21 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_EKON_lotes_abiertos_DataIngestion]
-- =============================================
-- Author:		Ignacio Morer
-- Create date: 15/03/2023
-- Description:	Procedimiento almacenado para la carga de la tabla [CINCAPORC_DW].[EKON].[lotes_abiertos] desde EKON
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
	SET @Aud_Tabla = 'lotes_abiertos'
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

		SET @Aud_Query = N'CREATE TABLE ' + @tabla_auditoria_integracion + ' (Accion varchar(10),
						 [Explotacion] [smallint]
						,[Rega] [varchar](25)
						,[Lote] [varchar](7) 
						,[fecha_apertura] [datetime] 
						,[origen] [varchar](40) 
						,[cantE] [decimal](38,4) 
						,[kilosE] [decimal](38,4) 
						,[importeE] [decimal](38,4) 
						,[cantS] [decimal](38,4) 
						,[kilosS] [decimal](38,4) 
						,[importeS] [decimal](38,4) 
						,[bajas] [decimal](38,4) 
						,[kilosP] [decimal](38,4) 
						,[importeP] [decimal](38,4) 
						,[costeP] [int] 
						,[transporte] [decimal](38,4) 
						,[medicamentos] [decimal](38,4) 
						,[costeInt] [decimal](38,4) 
						,[varios] [decimal](38,4) 
						,[otros] [decimal](38,4) 
						,[importegr] [decimal](38,4) 
						,[WTF] [smallint] 
						,[KPS_A] [decimal](38,4) 
						,[IPS_A] [decimal](38,4) 
						,[KPS_AF] [decimal](38,4) 
						,[IPS_AF] [decimal](38,4) 
						,[KPS_AR] [decimal](38,4) 
						,[IPS_AR] [decimal](38,4) 
						,[KPS_C] [decimal](38,4) 
						,[IPS_C] [decimal](38,4) 
						,[KPS_CE] [decimal](38,4) 
						,[IPS_CE] [decimal](38,4) 
						,[KPS_ISO] [decimal](38,4) 
						,[IPS_ISO] [decimal](38,4) 
						,[KPS_PRE] [decimal](38,4) 
						,[IPS_PRE] [decimal](38,4) 
						,[KPS_RA] [decimal](38,4) 
						,[IPS_RA] [decimal](38,4) 
						,[KPS_RC] [decimal](38,4) 
						,[IPS_RC] [decimal](38,4) 
						,[KPS_S] [decimal](38,4) 
						,[IPS_S] [decimal](38,4) 
						,[des_A] [varchar](7) 
						,[des_AF] [varchar](13) 
						,[des_AR] [varchar](8) 
						,[des_C] [varchar](11) 
						,[des_CE] [varchar](18) 
						,[des_ISO] [varchar](11) 
						,[des_PRE] [varchar](11) 
						,[des_RA] [varchar](14) 
						,[des_RC] [varchar](18) 
						,[des_S] [varchar](15) 
						,[listossal] [decimal](38,4) 
						,[Fecha_creacion] [datetime] 
						,[Usuario_creacion] [varchar](40) 
						,[Hostname_creacion] [varchar](35) 
						,[Fecha_modificacion] [datetime] 
						,[Usuario_modificacion] [varchar](40) 
						,[Hostname_modificacion] [varchar](35) 
						,[Flag_borrado] [bit]);'
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
		SET @Aud_Query = N'SELECT * INTO ' + @tabla_temporal + ' FROM OPENQUERY([' + @Servidor_origen + '], ''SELECT * FROM ' + @BBDD_origen + '.[IMP].[bi_lotes_abiertos]' + ''');' 
		
		EXEC sp_executesql @Aud_Query

		SET @Aud_Query = N'SELECT 
						[Explotacion],
				 		[Rega] COLLATE Modern_Spanish_CI_AS [Rega],
						[Lote] COLLATE Modern_Spanish_CI_AS [Lote],
						[fecha_apertura],
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
						[costeInt],
						[varios],
						[otros],
						[importegr],
						[WTF],
						[KPS_A],
					    [IPS_A], 
					    [KPS_AF],
						[IPS_AF], 
						[KPS_AR],
						[IPS_AR],
						[KPS_C], 
						[IPS_C], 
						[KPS_CE], 
						[IPS_CE], 
						[KPS_ISO], 
						[IPS_ISO], 
						[KPS_PRE], 
						[IPS_PRE], 
						[KPS_RA], 
						[IPS_RA], 
						[KPS_RC], 
						[IPS_RC], 
						[KPS_S], 
						[IPS_S], 
						[des_A] COLLATE Modern_Spanish_CI_AS [des_A],
						[des_AF] COLLATE Modern_Spanish_CI_AS [des_AF],
						[des_AR] COLLATE Modern_Spanish_CI_AS [des_AR],
						[des_C] COLLATE Modern_Spanish_CI_AS [des_C],
						[des_CE] COLLATE Modern_Spanish_CI_AS [des_CE],
						[des_ISO] COLLATE Modern_Spanish_CI_AS [des_ISO],
						[des_PRE] COLLATE Modern_Spanish_CI_AS [des_PRE],
						[des_RA] COLLATE Modern_Spanish_CI_AS [des_RA],
						[des_RC] COLLATE Modern_Spanish_CI_AS [des_RC],
						[des_S] COLLATE Modern_Spanish_CI_AS [des_S],
						[listossal] 
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
		EXEC('MERGE [CINCAPORC_DW].[EKON].[lotes_abiertos] AS TARGET
		USING ' + @tabla_temporal_final + ' AS SOURCE
		ON (TARGET.[Lote] = SOURCE.[Lote]) 
		
		WHEN NOT MATCHED BY TARGET
			THEN INSERT ([Explotacion],
				 		[Rega],
						[Lote],
						[fecha_apertura],
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
						[costeInt],
						[varios],
						[otros],
						[importegr],
						[WTF],
						[KPS_A],
					    [IPS_A], 
					    [KPS_AF],
						[IPS_AF], 
						[KPS_AR],
						[IPS_AR],
						[KPS_C], 
						[IPS_C], 
						[KPS_CE], 
						[IPS_CE], 
						[KPS_ISO], 
						[IPS_ISO], 
						[KPS_PRE], 
						[IPS_PRE], 
						[KPS_RA], 
						[IPS_RA], 
						[KPS_RC], 
						[IPS_RC], 
						[KPS_S], 
						[IPS_S], 
						[des_A],
						[des_AF],
						[des_AR],
						[des_C],
						[des_CE],
						[des_ISO],
						[des_PRE],
						[des_RA],
						[des_RC],
						[des_S],
						[listossal],
						[Fecha_creacion],
						[Usuario_creacion],
						[Hostname_creacion],
						[Fecha_modificacion],
						[Usuario_modificacion],
						[Hostname_modificacion],
						[Flag_borrado])
			VALUES (
					SOURCE.[Explotacion],
				 	SOURCE.[Rega],
					SOURCE.[Lote],
					SOURCE.[fecha_apertura],
					SOURCE.[origen],
					SOURCE.[cantE],
					SOURCE.[kilosE],
					SOURCE.[importeE],
					SOURCE.[cantS],
					SOURCE.[kilosS],
					SOURCE.[importeS],
					SOURCE.[bajas],
					SOURCE.[kilosP],
					SOURCE.[importeP],
					SOURCE.[costeP],
					SOURCE.[transporte],
					SOURCE.[medicamentos],
					SOURCE.[costeInt],
					SOURCE.[varios],
					SOURCE.[otros],
					SOURCE.[importegr],
					SOURCE.[WTF],
					SOURCE.[KPS_A],
					SOURCE.[IPS_A], 
					SOURCE.[KPS_AF],
					SOURCE.[IPS_AF], 
					SOURCE.[KPS_AR],
					SOURCE.[IPS_AR],
					SOURCE.[KPS_C], 
					SOURCE.[IPS_C], 
					SOURCE.[KPS_CE], 
					SOURCE.[IPS_CE], 
					SOURCE.[KPS_ISO], 
					SOURCE.[IPS_ISO], 
					SOURCE.[KPS_PRE], 
					SOURCE.[IPS_PRE], 
					SOURCE.[KPS_RA], 
					SOURCE.[IPS_RA], 
					SOURCE.[KPS_RC], 
					SOURCE.[IPS_RC], 
					SOURCE.[KPS_S], 
					SOURCE.[IPS_S], 
					SOURCE.[des_A],
					SOURCE.[des_AF],
					SOURCE.[des_AR],
					SOURCE.[des_C],
					SOURCE.[des_CE],
					SOURCE.[des_ISO],
					SOURCE.[des_PRE],
					SOURCE.[des_RA],
					SOURCE.[des_RC],
					SOURCE.[des_S],
					SOURCE.[listossal],
					''' + @Aud_FechaHora_Actual_String + '''
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
							OR ISNULL(NULLIF(TARGET.[costeInt], SOURCE.[costeInt]), NULLIF(SOURCE.[costeInt], TARGET.[costeInt])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[varios], SOURCE.[varios]), NULLIF(SOURCE.[varios], TARGET.[varios])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[otros], SOURCE.[otros]), NULLIF(SOURCE.[otros], TARGET.[otros])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[importegr], SOURCE.[importegr]), NULLIF(SOURCE.[importegr], TARGET.[importegr])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[WTF], SOURCE.[WTF]), NULLIF(SOURCE.[WTF], TARGET.[WTF])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_A], SOURCE.[KPS_A]), NULLIF(SOURCE.[KPS_A], TARGET.[KPS_A])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_A], SOURCE.[IPS_A]), NULLIF(SOURCE.[IPS_A], TARGET.[IPS_A])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_AF], SOURCE.[KPS_AF]), NULLIF(SOURCE.[KPS_AF], TARGET.[KPS_AF])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_AF], SOURCE.[IPS_AF]), NULLIF(SOURCE.[IPS_AF], TARGET.[IPS_AF])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_AR], SOURCE.[KPS_AR]), NULLIF(SOURCE.[KPS_AR], TARGET.[KPS_AR])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_AR], SOURCE.[IPS_AR]), NULLIF(SOURCE.[IPS_AR], TARGET.[IPS_AR])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_C], SOURCE.[KPS_C]), NULLIF(SOURCE.[KPS_C], TARGET.[KPS_C])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_C], SOURCE.[IPS_C]), NULLIF(SOURCE.[IPS_C], TARGET.[IPS_C])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_CE], SOURCE.[KPS_CE]), NULLIF(SOURCE.[KPS_CE], TARGET.[KPS_CE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_CE], SOURCE.[IPS_CE]), NULLIF(SOURCE.[IPS_CE], TARGET.[IPS_CE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_ISO], SOURCE.[KPS_ISO]), NULLIF(SOURCE.[KPS_ISO], TARGET.[KPS_ISO])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_ISO], SOURCE.[IPS_ISO]), NULLIF(SOURCE.[IPS_ISO], TARGET.[IPS_ISO])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_PRE], SOURCE.[KPS_PRE]), NULLIF(SOURCE.[KPS_PRE], TARGET.[KPS_PRE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_PRE], SOURCE.[IPS_PRE]), NULLIF(SOURCE.[IPS_PRE], TARGET.[IPS_PRE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_RA], SOURCE.[KPS_RA]), NULLIF(SOURCE.[KPS_RA], TARGET.[KPS_RA])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_RA], SOURCE.[IPS_RA]), NULLIF(SOURCE.[IPS_RA], TARGET.[IPS_RA])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_RC], SOURCE.[KPS_RC]), NULLIF(SOURCE.[KPS_RC], TARGET.[KPS_RC])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_RC], SOURCE.[IPS_RC]), NULLIF(SOURCE.[IPS_RC], TARGET.[IPS_RC])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[KPS_S], SOURCE.[KPS_S]), NULLIF(SOURCE.[KPS_S], TARGET.[KPS_S])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[IPS_S], SOURCE.[IPS_S]), NULLIF(SOURCE.[IPS_S], TARGET.[IPS_S])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_A], SOURCE.[des_A]), NULLIF(SOURCE.[des_A], TARGET.[des_A])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_AF], SOURCE.[des_AF]), NULLIF(SOURCE.[des_AF], TARGET.[des_AF])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_AR], SOURCE.[des_AR]), NULLIF(SOURCE.[des_AR], TARGET.[des_AR])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_C], SOURCE.[des_C]), NULLIF(SOURCE.[des_C], TARGET.[des_C])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_CE], SOURCE.[des_CE]), NULLIF(SOURCE.[des_CE], TARGET.[des_CE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_ISO], SOURCE.[des_ISO]), NULLIF(SOURCE.[des_ISO], TARGET.[des_ISO])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_PRE], SOURCE.[des_PRE]), NULLIF(SOURCE.[des_PRE], TARGET.[des_PRE])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_RA], SOURCE.[des_RA]), NULLIF(SOURCE.[des_RA], TARGET.[des_RA])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_RC], SOURCE.[des_RC]), NULLIF(SOURCE.[des_RC], TARGET.[des_RC])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[des_S], SOURCE.[des_S]), NULLIF(SOURCE.[des_S], TARGET.[des_S])) IS NOT NULL
							OR ISNULL(NULLIF(TARGET.[listossal], SOURCE.[listossal]), NULLIF(SOURCE.[listossal], TARGET.[listossal])) IS NOT NULL
							OR TARGET.[Flag_borrado] IS NOT NULL
		THEN UPDATE SET TARGET.[Explotacion] = SOURCE.[Explotacion]
						,TARGET.[Rega] = SOURCE.[Rega]
						,TARGET.[Lote] = SOURCE.[Lote]
						,TARGET.[fecha_apertura] = SOURCE.[fecha_apertura]
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
						,TARGET.[costeInt] = SOURCE.[costeInt]
						,TARGET.[varios] = SOURCE.[varios]
						,TARGET.[otros] = SOURCE.[otros]
						,TARGET.[importegr] = SOURCE.[importegr]
						,TARGET.[WTF] = SOURCE.[WTF]
						,TARGET.[KPS_A] = SOURCE.[KPS_A]
						,TARGET.[IPS_A] = SOURCE.[IPS_A]
						,TARGET.[KPS_AF] = SOURCE.[KPS_AF]
						,TARGET.[IPS_AF] = SOURCE.[IPS_AF]
						,TARGET.[KPS_AR] = SOURCE.[KPS_AR]
						,TARGET.[IPS_AR] = SOURCE.[IPS_AR]
						,TARGET.[KPS_C] = SOURCE.[KPS_C]
						,TARGET.[IPS_C] = SOURCE.[IPS_C]
						,TARGET.[KPS_CE] = SOURCE.[KPS_CE]
						,TARGET.[IPS_CE] = SOURCE.[IPS_CE]
						,TARGET.[KPS_ISO] = SOURCE.[KPS_ISO]
						,TARGET.[IPS_ISO] = SOURCE.[IPS_ISO]
						,TARGET.[KPS_PRE] = SOURCE.[KPS_PRE]
						,TARGET.[IPS_PRE] = SOURCE.[IPS_PRE]
						,TARGET.[KPS_RA] = SOURCE.[KPS_RA]
						,TARGET.[IPS_RA] = SOURCE.[IPS_RA]
						,TARGET.[KPS_RC] = SOURCE.[KPS_RC]
						,TARGET.[IPS_RC] = SOURCE.[IPS_RC]
						,TARGET.[KPS_S] = SOURCE.[KPS_S]
						,TARGET.[IPS_S] = SOURCE.[IPS_S]
						,TARGET.[des_A] = SOURCE.[des_A]
						,TARGET.[des_AF] = SOURCE.[des_AF]
						,TARGET.[des_AR] = SOURCE.[des_AR]
						,TARGET.[des_C] = SOURCE.[des_C]
						,TARGET.[des_CE] = SOURCE.[des_CE]
						,TARGET.[des_ISO] = SOURCE.[des_ISO]
						,TARGET.[des_PRE] = SOURCE.[des_PRE]
						,TARGET.[des_RA] = SOURCE.[des_RA]
						,TARGET.[des_RC] = SOURCE.[des_RC]
						,TARGET.[des_S] = SOURCE.[des_S]
						,TARGET.[listossal] = SOURCE.[listossal]
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
			CASE WHEN $action=''INSERT'' THEN INSERTED.[costeInt] ELSE DELETED.[costeInt] END AS [costeInt],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[varios] ELSE DELETED.[varios] END AS [varios],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[otros] ELSE DELETED.[otros] END AS [otros],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[importegr] ELSE DELETED.[importegr] END AS [importegr],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[WTF] ELSE DELETED.[WTF] END AS [WTF],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_A] ELSE DELETED.[KPS_A] END AS [KPS_A],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_A] ELSE DELETED.[IPS_A] END AS [IPS_A],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_AF] ELSE DELETED.[KPS_AF] END AS [KPS_AF],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_AF] ELSE DELETED.[IPS_AF] END AS [IPS_AF],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_AR] ELSE DELETED.[KPS_AR] END AS [KPS_AR],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_AR] ELSE DELETED.[IPS_AR] END AS [IPS_AR],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_C] ELSE DELETED.[KPS_C] END AS [KPS_C],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_C] ELSE DELETED.[IPS_C] END AS [IPS_C],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_CE] ELSE DELETED.[KPS_CE] END AS [KPS_CE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_CE] ELSE DELETED.[IPS_CE] END AS [IPS_CE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_ISO] ELSE DELETED.[KPS_ISO] END AS [KPS_ISO],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_ISO] ELSE DELETED.[IPS_ISO] END AS [IPS_ISO],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_PRE] ELSE DELETED.[KPS_PRE] END AS [KPS_PRE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_PRE] ELSE DELETED.[IPS_PRE] END AS [IPS_PRE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_RA] ELSE DELETED.[KPS_RA] END AS [KPS_RA],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_RA] ELSE DELETED.[IPS_RA] END AS [IPS_RA],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_RC] ELSE DELETED.[KPS_RC] END AS [KPS_RC],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_RC] ELSE DELETED.[IPS_RC] END AS [IPS_RC],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[KPS_S] ELSE DELETED.[KPS_S] END AS [KPS_S],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[IPS_S] ELSE DELETED.[IPS_S] END AS [IPS_S],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_A] ELSE DELETED.[des_A] END AS [des_A],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_AF] ELSE DELETED.[des_AF] END AS [des_AF],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_AR] ELSE DELETED.[des_AR] END AS [des_AR],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_C] ELSE DELETED.[des_C] END AS [des_C],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_CE] ELSE DELETED.[des_CE] END AS [des_CE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_ISO] ELSE DELETED.[des_ISO] END AS [des_ISO],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_PRE] ELSE DELETED.[des_PRE] END AS [des_PRE],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_RA] ELSE DELETED.[des_RA] END AS [des_RA],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_RC] ELSE DELETED.[des_RC] END AS [des_RC],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[des_S] ELSE DELETED.[des_S] END AS [des_S],
			CASE WHEN $action=''INSERT'' THEN INSERTED.[listossal] ELSE DELETED.[listossal] END AS [listossal],
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