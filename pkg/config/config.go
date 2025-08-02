package config

type Config struct {
	RootDir string        `yaml:"root_dir"`
	Trasher TrasherConfig `yaml:"trasher"`
}

type TrasherConfig struct {
	Workers                  int `yaml:"workers"`
	CollectorIterationsDelay int `yaml:"collector_iterations_delay"`
	WorkerIterationsDelay    int `yaml:"worker_iterations_delay"`
}
