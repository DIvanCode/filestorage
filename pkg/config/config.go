package config

type Config struct {
	Trasher TrasherConfig `yaml:"trasher"`
}

type TrasherConfig struct {
	Workers                  int `yaml:"workers"`
	CollectorIterationsDelay int `yaml:"collector_iterations_delay"`
	WorkerIterationsDelay    int `yaml:"worker_iterations_delay"`
}
