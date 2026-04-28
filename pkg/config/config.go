package config

type Config struct {
	RootDir string        `yaml:"root_dir" env:"ROOT_DIR"`
	Trasher TrasherConfig `yaml:"trasher" env-prefix:"TRASHER_"`
}

type TrasherConfig struct {
	Workers                  int `yaml:"workers" env:"WORKERS"`
	CollectorIterationsDelay int `yaml:"collector_iterations_delay" env:"COLLECTOR_ITERATIONS_DELAY"`
	WorkerIterationsDelay    int `yaml:"worker_iterations_delay" env:"WORKER_ITERATIONS_DELAY"`
}
