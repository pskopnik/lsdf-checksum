package ledis

func (l *LedisDBConfig) CopyFrom(other *LedisDBConfig) {
	l.Address = other.Address
	l.AuthPassword = other.AuthPassword

	l.ConnReadBufferSize = other.ConnReadBufferSize
	l.ConnWriteBufferSize = other.ConnWriteBufferSize
	l.ConnKeepaliveInterval = other.ConnKeepaliveInterval

	l.TTLCheckInterval = other.TTLCheckInterval

	l.Compression = other.Compression
	l.BlockSize = other.BlockSize
	l.WriteBufferSize = other.WriteBufferSize
	l.CacheSize = other.CacheSize

	l.Logger = other.Logger
}

func (l *LedisDBConfig) Merge(other *LedisDBConfig) *LedisDBConfig {
	if len(other.Address) > 0 {
		l.Address = other.Address
	}
	if len(other.AuthPassword) > 0 {
		l.AuthPassword = other.AuthPassword
	}

	if l.ConnReadBufferSize != 0 {
		l.ConnReadBufferSize = other.ConnReadBufferSize
	}
	if l.ConnWriteBufferSize != 0 {
		l.ConnWriteBufferSize = other.ConnWriteBufferSize
	}
	if l.ConnKeepaliveInterval != 0 {
		l.ConnKeepaliveInterval = other.ConnKeepaliveInterval
	}

	if l.TTLCheckInterval != 0 {
		l.TTLCheckInterval = other.TTLCheckInterval
	}

	if l.Compression {
		l.Compression = other.Compression
	}
	if l.BlockSize != 0 {
		l.BlockSize = other.BlockSize
	}
	if l.WriteBufferSize != 0 {
		l.WriteBufferSize = other.WriteBufferSize
	}
	if l.CacheSize != 0 {
		l.CacheSize = other.CacheSize
	}

	if other.Logger != nil {
		l.Logger = other.Logger
	}

	return l
}

func (l *LedisDBConfig) Clone() *LedisDBConfig {
	config := &LedisDBConfig{}
	config.CopyFrom(l)
	return config
}
