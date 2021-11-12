package config

const (
	BLOCK_RESTART_INTERVAL = 16	// 重新进行前缀压缩的最大key间隔，参考leveldb设置为16
	BLOCK_MAX_SIZE = 4 * 1024	// 最大的块大小，超过该限制后应该及时刷到磁盘上
)