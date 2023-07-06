USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[usp_Alertas]    Script Date: 20/03/2023 16:26:26 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[usp_Alertas]
-- =============================================
-- Author:		Marcos Ayuso
-- Create date: 09/03/2023
-- Description:	Procedimiento almacenado wrapper para la gestión de las alertas
-- =============================================
	--Definición parámetros
	@Cod_Alerta varchar(50),
	@Query_alertas nvarchar(MAX),
	@Asunto varchar(300)

AS
BEGIN
	SET NOCOUNT ON

	--Inicialización de variables de auditoría
	DECLARE @Aud_Tabla varchar(250),
			@Aud_Esquema varchar(5),
			@Aud_Fecha_Inicio datetime,
			@Aud_Usuario varchar(50),
			@Aud_Hostname varchar(50)
	SET @Aud_Tabla = 'tbl_alerta_registro'
	SET @Aud_Esquema = 'dbo'
	SET @Aud_Fecha_Inicio = getdate()
	SET @Aud_Usuario = SYSTEM_USER
	SET @Aud_Hostname = HOST_NAME()

	--Inicialización del mensaje de auditoría
	DECLARE @ErrorNumber varchar(15),
	@ErrorState varchar(15),
	@ErrorSeverity varchar(15),
	@ErrorProcedure varchar(50),
	@ErrorLine varchar(15),
	@ErrorMessage varchar(300)

	DECLARE @StoredProcedure VARCHAR(200),
	        @Codigo VARCHAR(50),
	        @TipoAlerta VARCHAR(5),
			@Destinatario VARCHAR(MAX),
	        @Clave varchar(MAX) = NULL,
	        @Datos varchar(MAX) = NULL,
			@Destino varchar(MAX) = NULL,
			@Detalle varchar(MAX),
			@Formato varchar(20)

	BEGIN TRY
		
		CREATE TABLE #taux (Clave varchar(200), Datos varchar(MAX), Destino varchar(400))
		
		EXEC sp_executesql @Query_alertas
		
		DECLARE Alertas CURSOR FOR 
		
		SELECT * FROM #taux

		OPEN Alertas
		
		FETCH NEXT FROM Alertas INTO @Clave, @Datos, @Destino

		WHILE @@fetch_status = 0
		BEGIN

			INSERT INTO [CINCAPORC_DW].[dbo].[tbl_alerta_registro]([Codigo],[Clave],[Destino],[Datos],[Estado],[Fecha_Creacion],[Fecha_Envio]) 
			       VALUES (@Cod_Alerta, @Clave, @Destino, @Datos, 'Pendiente', getdate(), NULL)
			
			FETCH NEXT FROM Alertas INTO @Clave, @Datos, @Destino
		END
		
		CLOSE Alertas
		DEALLOCATE Alertas

	END TRY
	BEGIN CATCH

		SELECT @ErrorNumber = CAST(ERROR_NUMBER() AS varchar), @ErrorState = CAST(ERROR_STATE() AS varchar), @ErrorSeverity = CAST(ERROR_SEVERITY() AS varchar), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorLine = CAST(ERROR_LINE() AS varchar), @ErrorMessage = REPLACE(ERROR_MESSAGE(),'''','"');
		
		--Mensaje de auditoría
		EXEC dbo.usp_OutputErrorMessage @Aud_Tabla, @Aud_Esquema, @Aud_Fecha_Inicio, 9, NULL, @ErrorNumber, @ErrorState, @ErrorSeverity, @ErrorProcedure, @ErrorLine, @ErrorMessage
	
		RETURN -1
		
	END CATCH;

END
GO