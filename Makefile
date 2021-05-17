MODULE_NAME=github.com/goleveldb/goleveldb
MOCK_HOME=./internal/mock

# 生成 file 包 Writer 接口的 mock 文件
gen_mock_file_writer:
	mockgen -destination ${MOCK_HOME}/file/writer.go -package file  ${MODULE_NAME}/file Writer

clean_mock:
	rm -rf ./internal/mock