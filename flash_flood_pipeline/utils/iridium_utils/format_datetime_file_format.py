def format_datetime_file_format(dt):
    """Format the datetime in 'yymmdd_hhnnss.zzz' format."""
    return dt.strftime("%y%m%d%H%M%S")
