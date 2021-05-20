MODULE_NAME=github.com/goleveldb/goleveldb
MOCK_HOME=./internal/mock

# 生成 file 包 Writer 接口的 mock 文件
gen_mock_file_writer:
	mockgen -destination ${MOCK_HOME}/file/writer.go -package file  ${MODULE_NAME}/file Writer

# 生成 file包 接口的 mock 文件
gen_mock_file:
	mockgen -destination ${MOCK_HOME}/mock_file/sequential_reader_mock.go ${MODULE_NAME}/file SequentialReader
	mockgen -destination ${MOCK_HOME}/mock_file/writer_mock.go ${MODULE_NAME}/file Writer

gen_mock_log_reporter:
	mockgen -destination ${MOCK_HOME}/mock_log/reporter_mock.go ${MODULE_NAME}/log Reporter


clean_mock:
	rm -rf ./internal/mock