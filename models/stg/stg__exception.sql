select COMMSHUB_CD,
	DELETE_FLAG ,
	COMMSHUB_KEY,
	AFFECTED_PM ,
	EFFECTIVE_FROM_DT_TM,
	LOAD_DT_TM,
	EXCEPTION_CODE,
	EFFECTIVE_TO_DT_TM  from {{ source('smart_meters_files','exception')}}