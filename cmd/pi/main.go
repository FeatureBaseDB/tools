package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/pilosa/go-pilosa"
	"github.com/pilosa/tools"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func main() {
	go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()

	if err := NewRootCommand().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func NewRootCommand() *cobra.Command {
	rc := &cobra.Command{
		Use:   "pi",
		Short: "Pilosa Tools",
		Long: `Contains various benchmarking and cluster creation and management tools for
Pilosa. Try "pi <command> --help for more information."

Version: ` + tools.Version + `
Build Time: ` + tools.BuildTime + "\n",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			v := viper.New()
			err := setAllConfig(v, cmd.Flags(), "PI")
			if err != nil {
				return err
			}

			// return "dry run" error if "dry-run" flag is set
			if ret, err := cmd.Flags().GetBool("dry-run"); ret && err == nil {
				if cmd.Parent() != nil {
					return fmt.Errorf("dry run")
				} else if err != nil {
					return fmt.Errorf("problem getting dry-run flag: %v", err)
				}
			}

			return nil
		},
	}
	rc.PersistentFlags().BoolP("verbose", "v", false, "Enable verbose logging.")
	rc.PersistentFlags().Bool("dry-run", false, "Stop before executing. Useful for testing.")
	_ = rc.PersistentFlags().MarkHidden("dry-run")
	rc.PersistentFlags().StringP("config", "c", "", "Configuration file to read from.")
	_ = rc.PersistentFlags().MarkHidden("config")

	rc.AddCommand(NewBenchCommand())
	rc.AddCommand(NewReplayCommand())
	rc.AddCommand(NewImportFragsCommand())

	rc.SetOutput(os.Stderr)
	return rc
}

// setAllConfig takes a FlagSet to be the definition of all configuration
// options, as well as their defaults. It then reads from the command line, the
// environment, and a config file (if specified), and applies the configuration
// in that priority order. Since each flag in the set contains a pointer to
// where its value should be stored, setAllConfig can directly modify the value
// of each config variable.
//
// setAllConfig looks for environment variables which are capitalized versions
// of the flag names with dashes replaced by underscores, and prefixed with
// envPrefix plus an underscore.
func setAllConfig(v *viper.Viper, flags *pflag.FlagSet, envPrefix string) error {
	// add cmd line flag def to viper
	err := v.BindPFlags(flags)
	if err != nil {
		return err
	}

	// add env to viper
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	c := v.GetString("config")
	var flagErr error
	validTags := make(map[string]bool)
	flags.VisitAll(func(f *pflag.Flag) {
		validTags[f.Name] = true
	})

	// add config file to viper
	if c != "" {
		v.SetConfigFile(c)
		v.SetConfigType("toml")
		err := v.ReadInConfig()
		if err != nil {
			return fmt.Errorf("error reading configuration file '%s': %v", c, err)
		}

		for _, key := range v.AllKeys() {
			if _, ok := validTags[key]; !ok {
				return fmt.Errorf("invalid option in configuration file: %v", key)
			}
		}

	}

	// set all values from viper
	flags.VisitAll(func(f *pflag.Flag) {
		if flagErr != nil {
			return
		}
		var value string
		if f.Value.Type() == "stringSlice" {
			// special handling is needed for stringSlice as v.GetString will
			// always return "" in the case that the value is an actual string
			// slice from a config file rather than a comma separated string
			// from a flag or env var.
			vss := v.GetStringSlice(f.Name)
			value = strings.Join(vss, ",")
		} else {
			value = v.GetString(f.Name)
		}

		if f.Changed {
			// If f.Changed is true, that means the value has already been set
			// by a flag, and we don't need to ask viper for it since the flag
			// is the highest priority. This works around a problem with string
			// slices where f.Value.Set(csvString) would cause the elements of
			// csvString to be appended to the existing value rather than
			// replacing it.
			return
		}
		flagErr = f.Value.Set(value)
	})
	return flagErr
}

// NewClientFromFlags returns a new Pilosa client based on the flag arguments.
func NewClientFromFlags(flags *pflag.FlagSet) (*pilosa.Client, error) {
	hosts, err := flags.GetStringSlice("hosts")
	if err != nil {
		return nil, err
	}
	tlsSkipVerify, err := flags.GetBool("tls.skip-verify")
	if err != nil {
		return nil, err
	}
	clientOptions := []pilosa.ClientOption{
		pilosa.OptClientTLSConfig(&tls.Config{InsecureSkipVerify: tlsSkipVerify}),
	}
	return pilosa.NewClient(hosts, clientOptions...)
}

func NewLoggerFromFlags(flags *pflag.FlagSet) *log.Logger {
	if verbose, _ := flags.GetBool("verbose"); verbose {
		return log.New(os.Stderr, "", log.LstdFlags)
	}
	return log.New(ioutil.Discard, "", log.LstdFlags)
}
