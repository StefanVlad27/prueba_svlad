USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[usp_Download_Validation]    Script Date: 20/03/2023 16:27:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[usp_Download_Validation]
-- =============================================
-- Author:		Marcos Ayuso
-- Create date: 06/03/2023
-- Description:	Procedimiento almacenado para la validación de la proporción de registros
-- =============================================
	--Definición parámetros
	@Aud_Tabla varchar(250),
	@Aud_Esquema varchar(10),
	@Aud_Elemento_Auditado varchar(255) = ''

AS
BEGIN
	SET NOCOUNT ON
	
	--Inicialización de variables de auditoría
	DECLARE @Aud_Fecha_Inicio datetime,
			@FiltroMatadero varchar(250) = '',
			@Aud_Usuario varchar(50),
			@Aud_Hostname varchar(50),
			@Aud_FechaHora_Actual datetime,
			@Aud_FechaHora_Actual_String varchar(30)
			
	IF @Aud_Elemento_Auditado = ''
		SET @Aud_Elemento_Auditado = 'usp_' + @Aud_Esquema + '_' + @Aud_Tabla + '_DataIngestion'
	
	SET @Aud_Fecha_Inicio = getdate()
	SET @Aud_Usuario = SYSTEM_USER
	SET @Aud_Hostname = HOST_NAME()
	
	--Inicialización del mensaje de auditoría
	DECLARE @Aud_Mensaje varchar(600),
			@Aud_Asunto varchar(600)
	
	--Inicialización del mensaje de error
	DECLARE @ErrorNumber varchar(15),
			@ErrorState varchar(15),
			@ErrorSeverity varchar(15),
			@ErrorProcedure varchar(50),
			@ErrorLine varchar(15),
			@ErrorMessage varchar(300)

	BEGIN TRY

		--Mensaje de auditoría
		EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 18, @Aud_Elemento_Auditado
	
		IF 0 = (SELECT COUNT(*)
				FROM [AUD].[tbl_Downloaded_Volumetry_Alerts]
				WHERE Estado IN ('Alerta','Excepción') AND Elemento_Auditado = @Aud_Elemento_Auditado)
			BEGIN
				--DROP TABLE IF EXISTS [TMP].[tbl_Downloaded_Volumetry_Validation]
				DELETE FROM [TMP].[tbl_Downloaded_Volumetry_Validation] WHERE Elemento_Auditado = @Aud_Elemento_Auditado
				INSERT INTO [TMP].[tbl_Downloaded_Volumetry_Validation]
				SELECT h.*,
						a.Ins_dia_anterior,
						CASE WHEN ABS(h.Ins_dia_actual-a.Ins_dia_anterior)>h.Ins_dia_actual*0.3 THEN 'Alerta' ELSE 'Correcto' END AS Comp_dia_ant,
						s.Ins_dia_semana_anterior,
						CASE WHEN ABS(h.Ins_dia_actual-s.Ins_dia_semana_anterior)>h.Ins_dia_actual*0.3 THEN 'Alerta' ELSE 'Correcto' END AS Comp_dia_sem_ant,
						ms.Ins_media_ult_7dias,
						CASE WHEN ABS(h.Ins_dia_actual-ms.Ins_media_ult_7dias)>h.Ins_dia_actual*0.3 THEN 'Alerta' ELSE 'Correcto' END AS Comp_sem_ant,
						mm.Ins_media_ult_30dias,
						CASE WHEN ABS(h.Ins_dia_actual-mm.Ins_media_ult_30dias)>h.Ins_dia_actual*0.3 THEN 'Alerta' ELSE 'Correcto' END AS Comp_mes_ant
				FROM (SELECT Elemento_Auditado, Fecha_carga, SUM(Registros_insertados) AS Ins_dia_actual
						FROM AUD.tbl_Audit_Volumetry
						WHERE Elemento_Auditado=@Aud_Elemento_Auditado
							AND Fecha_carga=CAST(GETDATE() AS date)
						GROUP BY Elemento_Auditado, Fecha_carga) h
				FULL OUTER JOIN (SELECT t1.Elemento_Auditado, SUM(Registros_insertados) AS Ins_dia_anterior
									FROM AUD.tbl_Audit_Volumetry t1
									INNER JOIN (SELECT Elemento_Auditado, MAX(CAST(Fecha_carga AS date)) Fecha
												FROM AUD.tbl_Audit_Volumetry
												WHERE Fecha_carga<>CAST(GETDATE() AS date)
												GROUP BY Elemento_Auditado) t2
										ON t1.Elemento_Auditado=t2.Elemento_Auditado AND  t1.Fecha_carga=t2.Fecha
									WHERE t1.Elemento_Auditado=@Aud_Elemento_Auditado
									GROUP BY t1.Elemento_Auditado) a
					ON a.Elemento_Auditado=h.Elemento_Auditado
				FULL OUTER JOIN (SELECT Elemento_Auditado, SUM(Registros_insertados) AS Ins_dia_semana_anterior
									FROM AUD.tbl_Audit_Volumetry
									WHERE Elemento_Auditado=@Aud_Elemento_Auditado
										AND Fecha_carga=CAST(DATEADD(day,-7,GETDATE()) AS date)
									GROUP BY Elemento_Auditado) s
					ON a.Elemento_Auditado=h.Elemento_Auditado
				FULL OUTER JOIN (SELECT Elemento_Auditado, AVG(Registros_insertados) AS Ins_media_ult_7dias
									FROM (SELECT CAST(DiaHora AS date) AS Fecha, Elemento_Auditado, SUM(Registros_insertados) AS Registros_insertados
									        FROM AUD.tbl_Audit_Volumetry
									        WHERE Elemento_Auditado=@Aud_Elemento_Auditado
										        AND Fecha_carga<CAST(GETDATE() AS date)
										        AND Fecha_carga>CAST(DATEADD(day,-8,GETDATE()) AS date)
									        GROUP BY CAST(DiaHora AS date), Elemento_Auditado) t
									GROUP BY Elemento_Auditado) ms
					ON ms.Elemento_Auditado=h.Elemento_Auditado
				FULL OUTER JOIN (SELECT Elemento_Auditado, AVG(Registros_insertados) AS Ins_media_ult_30dias
									FROM (SELECT CAST(DiaHora AS date) AS Fecha, Elemento_Auditado, SUM(Registros_insertados) AS Registros_insertados
									        FROM AUD.tbl_Audit_Volumetry
									        WHERE Elemento_Auditado=@Aud_Elemento_Auditado
										        AND Fecha_carga<CAST(GETDATE() AS date)
										        AND Fecha_carga>CAST(DATEADD(day,-31,GETDATE()) AS date)
									        GROUP BY CAST(DiaHora AS date), Elemento_Auditado) t
									GROUP BY Elemento_Auditado) mm
					ON mm.Elemento_Auditado=h.Elemento_Auditado
				
				--Instante de tiempo en el que se realiza el merge
				SET @Aud_FechaHora_Actual = getdate()
		
				INSERT INTO [AUD].[tbl_Downloaded_Volumetry_Alerts]
				SELECT *, 'Alerta' AS Estado, @Aud_FechaHora_Actual AS Fecha_creacion, @Aud_Usuario AS Usuario, @Aud_Hostname AS Hostname
				FROM [TMP].[tbl_Downloaded_Volumetry_Validation]
				WHERE Comp_dia_ant='Alerta' AND Comp_dia_sem_ant='Alerta' AND Comp_sem_ant='Alerta' AND Comp_mes_ant='Alerta' AND (Ins_dia_actual>100 OR Ins_dia_anterior>100 OR Ins_dia_semana_anterior>100 OR Ins_media_ult_7dias>100 OR Ins_media_ult_30dias>100) AND Elemento_Auditado = @Aud_Elemento_Auditado
			END

		SET @Aud_Asunto = 'Detectada carga anómala en la tabla ' + @Aud_Esquema + '.' + @Aud_Tabla + '[' + OBJECT_NAME(@@PROCID) + ']' 
		SET @Aud_Mensaje = 'Existe una anomalía en las volumetrías cargadas en la tabla ' + @Aud_Esquema + '.' + @Aud_Tabla

		IF 0 < (SELECT COUNT(*)
				FROM [AUD].[tbl_Downloaded_Volumetry_Alerts]
				WHERE Estado='Alerta' AND Elemento_Auditado = @Aud_Elemento_Auditado)
			BEGIN
				EXEC msdb.dbo.sp_send_dbmail
				@profile_name = 'ses-service',
				@recipients = 'alertas-sql@iasapiens.com',
				@subject = @Aud_Asunto,
				@body = @Aud_Mensaje;
			END

		EXEC dbo.usp_OutputInfoMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 19, @Aud_Elemento_Auditado
			
	END TRY
	BEGIN CATCH

		SELECT @ErrorNumber = CAST(ERROR_NUMBER() AS varchar), @ErrorState = CAST(ERROR_STATE() AS varchar), @ErrorSeverity = CAST(ERROR_SEVERITY() AS varchar), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorLine = CAST(ERROR_LINE() AS varchar), @ErrorMessage = REPLACE(ERROR_MESSAGE(),'''','"');
	
		--Mensaje de auditoría
		EXEC dbo.usp_OutputErrorMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 8, @ErrorNumber, @ErrorState, @ErrorSeverity, @ErrorProcedure, @ErrorLine, @ErrorMessage

		RETURN -1
	
	END CATCH;

END
GO