-record(wal, {
              file_pos = 0 :: non_neg_integer(),
              file_buff = [] :: term(),
              file_desc :: file:fd() | undefined,
              file_status :: more_data | eof,
              file_name :: file:filename_all()
             }).
