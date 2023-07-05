USE [CINCAPORC_DW]
GO

/****** Object:  StoredProcedure [dbo].[spSendEmail]    Script Date: 20/03/2023 16:23:05 ******/
SET ANSI_NULLS OFF
GO

SET QUOTED_IDENTIFIER OFF
GO

CREATE PROCEDURE [dbo].[spSendEmail]
	@EmailTo [nvarchar](100),
	@Subject [nvarchar](100),
	@Body [nvarchar](max),
	@EmailFrom [nvarchar](100)
WITH EXECUTE AS CALLER
AS
EXTERNAL NAME [SendDatabaseMail4].[SendDatabaseMail4.SendMail_ClassLib].[SendEmailUsingCLR]
GO