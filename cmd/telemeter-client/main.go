package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/spf13/cobra"

	"github.com/openshift/telemeter/pkg/authorizer/remote"
	"github.com/openshift/telemeter/pkg/forwarder"
	telemeterhttp "github.com/openshift/telemeter/pkg/http"
	"github.com/openshift/telemeter/pkg/metricfamily"
	"github.com/openshift/telemeter/pkg/metricsclient"
)

func main() {
	opt := &Options{
		Listen:     "localhost:9002",
		LimitBytes: 200 * 1024,
		Rules:      []string{`{__name__="up"}`},
		Interval:   4*time.Minute + 30*time.Second,
	}
	cmd := &cobra.Command{
		Short: "Federate Prometheus via push",

		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return opt.Run()
		},
	}

	cmd.Flags().StringVar(&opt.Listen, "listen", opt.Listen, "A host:port to listen on for health and metrics.")
	cmd.Flags().StringVar(&opt.From, "from", opt.From, "The Prometheus server to federate from.")
	cmd.Flags().StringVar(&opt.FromToken, "from-token", opt.FromToken, "A bearer token to use when authenticating to the source Prometheus server.")
	cmd.Flags().StringVar(&opt.FromCAFile, "from-ca-file", opt.FromCAFile, "A file containing the CA certificate to use to verify the --from URL in addition to the system roots certificates.")
	cmd.Flags().StringVar(&opt.FromTokenFile, "from-token-file", opt.FromTokenFile, "A file containing a bearer token to use when authenticating to the source Prometheus server.")
	cmd.Flags().StringVar(&opt.Identifier, "id", opt.Identifier, "The unique identifier for metrics sent with this client.")
	cmd.Flags().StringVar(&opt.To, "to", opt.To, "A telemeter server to send metrics to.")
	cmd.Flags().StringVar(&opt.ToUpload, "to-upload", opt.ToUpload, "A telemeter server endpoint to push metrics to. Will be defaulted for standard servers.")
	cmd.Flags().StringVar(&opt.ToAuthorize, "to-auth", opt.ToAuthorize, "A telemeter server endpoint to exchange the bearer token for an access token. Will be defaulted for standard servers.")
	cmd.Flags().StringVar(&opt.ToToken, "to-token", opt.ToToken, "A bearer token to use when authenticating to the destination telemeter server.")
	cmd.Flags().StringVar(&opt.ToTokenFile, "to-token-file", opt.ToTokenFile, "A file containing a bearer token to use when authenticating to the destination telemeter server.")
	cmd.Flags().DurationVar(&opt.Interval, "interval", opt.Interval, "The interval between scrapes. Prometheus returns the last 5 minutes of metrics when invoking the federation endpoint.")

	// TODO: more complex input definition, such as a JSON struct
	cmd.Flags().StringArrayVar(&opt.Rules, "match", opt.Rules, "Match rules to federate.")
	cmd.Flags().StringVar(&opt.RulesFile, "match-file", opt.RulesFile, "A file containing match rules to federate, one rule per line.")

	cmd.Flags().StringArrayVar(&opt.LabelFlag, "label", opt.LabelFlag, "Labels to add to each outgoing metric, in key=value form.")
	cmd.Flags().StringSliceVar(&opt.RenameFlag, "rename", opt.RenameFlag, "Rename metrics before sending by specifying OLD=NEW name pairs. Defaults to renaming ALERTS to alerts. Defaults to ALERTS=alerts.")

	cmd.Flags().StringArrayVar(&opt.AnonymizeLabels, "anonymize-labels", opt.AnonymizeLabels, "Anonymize the values of the provided values before sending them on.")
	cmd.Flags().StringVar(&opt.AnonymizeSalt, "anonymize-salt", opt.AnonymizeSalt, "A secret and unguessable value used to anonymize the input data.")
	cmd.Flags().StringVar(&opt.AnonymizeSaltFile, "anonymize-salt-file", opt.AnonymizeSaltFile, "A file containing a secret and unguessable value used to anonymize the input data.")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type Options struct {
	Listen     string
	LimitBytes int64

	From          string
	To            string
	ToUpload      string
	ToAuthorize   string
	FromCAFile    string
	FromToken     string
	FromTokenFile string
	ToToken       string
	ToTokenFile   string
	Identifier    string

	RenameFlag []string
	Renames    map[string]string

	AnonymizeLabels   []string
	AnonymizeSalt     string
	AnonymizeSaltFile string

	Rules     []string
	RulesFile string

	LabelFlag []string
	Labels    map[string]string

	Interval time.Duration

	LabelRetriever metricfamily.LabelRetriever
}

func (o *Options) Transforms() []metricfamily.Transformer {
	var transforms metricfamily.AllTransformer
	if len(o.Labels) > 0 || o.LabelRetriever != nil {
		transforms = append(transforms, metricfamily.NewLabel(o.Labels, o.LabelRetriever))
	}
	if len(o.AnonymizeLabels) > 0 {
		transforms = append(transforms, metricfamily.NewMetricsAnonymizer(o.AnonymizeSalt, o.AnonymizeLabels, nil))
	}
	if len(o.Renames) > 0 {
		transforms = append(transforms, metricfamily.RenameMetrics{Names: o.Renames})
	}
	transforms = append(transforms,
		metricfamily.NewDropInvalidFederateSamples(time.Now().Add(-24*time.Hour)),
		metricfamily.TransformerFunc(metricfamily.PackMetrics),
		metricfamily.TransformerFunc(metricfamily.SortMetrics),
	)
	return []metricfamily.Transformer{transforms}
}

func (o *Options) MatchRules() []string {
	return o.Rules
}

func (o *Options) Run() error {
	if len(o.From) == 0 {
		return fmt.Errorf("you must specify a Prometheus server to federate from (e.g. http://localhost:9090)")
	}

	if len(o.ToToken) == 0 && len(o.ToTokenFile) > 0 {
		data, err := ioutil.ReadFile(o.ToTokenFile)
		if err != nil {
			return fmt.Errorf("unable to read --to-token-file: %v", err)
		}
		o.ToToken = strings.TrimSpace(string(data))
	}
	if len(o.FromToken) == 0 && len(o.FromTokenFile) > 0 {
		data, err := ioutil.ReadFile(o.FromTokenFile)
		if err != nil {
			return fmt.Errorf("unable to read --from-token-file: %v", err)
		}
		o.FromToken = strings.TrimSpace(string(data))
	}
	if len(o.AnonymizeSalt) == 0 && len(o.AnonymizeSaltFile) > 0 {
		data, err := ioutil.ReadFile(o.AnonymizeSaltFile)
		if err != nil {
			return fmt.Errorf("unable to read --anonymize-salt-file: %v", err)
		}
		o.AnonymizeSalt = strings.TrimSpace(string(data))
	}

	if len(o.AnonymizeLabels) > 0 && len(o.AnonymizeSalt) == 0 {
		return fmt.Errorf("you must specify --anonymize-salt when --anonymize-labels is used")
	}
	for _, flag := range o.LabelFlag {
		values := strings.SplitN(flag, "=", 2)
		if len(values) != 2 {
			return fmt.Errorf("--label must be of the form key=value: %s", flag)
		}
		if o.Labels == nil {
			o.Labels = make(map[string]string)
		}
		o.Labels[values[0]] = values[1]
	}

	if len(o.RenameFlag) == 0 {
		o.RenameFlag = []string{"ALERTS=alerts"}
	}
	for _, flag := range o.RenameFlag {
		if len(flag) == 0 {
			continue
		}
		values := strings.SplitN(flag, "=", 2)
		if len(values) != 2 {
			return fmt.Errorf("--rename must be of the form OLD_NAME=NEW_NAME: %s", flag)
		}
		if o.Renames == nil {
			o.Renames = make(map[string]string)
		}
		o.Renames[values[0]] = values[1]
	}

	if len(o.RulesFile) > 0 {
		data, err := ioutil.ReadFile(o.RulesFile)
		if err != nil {
			return fmt.Errorf("--match-file could not be loaded: %v", err)
		}
		o.Rules = append(o.Rules, strings.Split(string(data), "\n")...)
	}
	var rules []string
	for _, s := range o.Rules {
		s = strings.TrimSpace(s)
		if len(s) == 0 {
			continue
		}
		rules = append(rules, s)
	}
	o.Rules = rules

	from, err := url.Parse(o.From)
	if err != nil {
		return fmt.Errorf("--from is not a valid URL: %v", err)
	}
	from.Path = strings.TrimRight(from.Path, "/")
	if len(from.Path) == 0 {
		from.Path = "/federate"
	}

	var to, toUpload, toAuthorize *url.URL
	if len(o.ToUpload) > 0 {
		to, err = url.Parse(o.ToUpload)
		if err != nil {
			return fmt.Errorf("--to is not a valid URL: %v", err)
		}
	}
	if len(o.ToAuthorize) > 0 {
		toAuthorize, err = url.Parse(o.ToAuthorize)
		if err != nil {
			return fmt.Errorf("--to-auth is not a valid URL: %v", err)
		}
	}
	if len(o.To) > 0 {
		to, err = url.Parse(o.To)
		if err != nil {
			return fmt.Errorf("--to is not a valid URL: %v", err)
		}
		if len(to.Path) == 0 {
			to.Path = "/"
		}
		if toAuthorize == nil {
			u := *to
			u.Path = path.Join(to.Path, "authorize")
			if len(o.Identifier) > 0 {
				q := to.Query()
				q.Add("id", o.Identifier)
				u.RawQuery = q.Encode()
			}
			toAuthorize = &u
		}
		if toUpload == nil {
			u := *to
			u.Path = path.Join(to.Path, "upload")
			toUpload = &u
		}
	}

	if toUpload == nil || toAuthorize == nil {
		return fmt.Errorf("either --to or --to-auth and --to-upload must be specified")
	}

	fromTransport := metricsclient.DefaultTransport()
	if len(o.FromCAFile) > 0 {
		if fromTransport.TLSClientConfig == nil {
			fromTransport.TLSClientConfig = &tls.Config{}
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			return fmt.Errorf("can't read system certificates when --from-ca-file was specified: %v", err)
		}
		data, err := ioutil.ReadFile(o.FromCAFile)
		if err != nil {
			return fmt.Errorf("can't read --from-ca-file: %v", err)
		}
		if !pool.AppendCertsFromPEM(data) {
			log.Printf("warning: No certs found in --from-ca-file")
		}
		fromTransport.TLSClientConfig.RootCAs = pool
	}
	fromClient := &http.Client{Transport: fromTransport}
	if len(o.FromToken) > 0 {
		fromClient.Transport = telemeterhttp.NewBearerRoundTripper(o.FromToken, fromClient.Transport)
	}
	toClient := &http.Client{Transport: metricsclient.DefaultTransport()}
	if len(o.ToToken) > 0 {
		// exchange our token for a token from the authorize endpoint, which also gives us a
		// set of expected labels we must include
		rt := remote.NewServerRotatingRoundTripper(o.ToToken, toAuthorize, toClient.Transport)
		o.LabelRetriever = rt
		toClient.Transport = rt
	}

	worker := forwarder.New(*from, toUpload, o)
	worker.ToClient = metricsclient.New(toClient, o.LimitBytes, o.Interval, "federate_to")
	worker.FromClient = metricsclient.New(fromClient, o.LimitBytes, o.Interval, "federate_from")
	worker.Interval = o.Interval

	log.Printf("Starting telemeter-client reading from %s and sending to %s (listen=%s)", o.From, o.To, o.Listen)

	go worker.Run()

	if len(o.Listen) > 0 {
		handlers := http.NewServeMux()
		telemeterhttp.AddDebug(handlers)
		telemeterhttp.AddHealth(handlers)
		telemeterhttp.AddMetrics(handlers)
		handlers.Handle("/federate", serveLastMetrics(worker))
		go func() {
			if err := http.ListenAndServe(o.Listen, handlers); err != nil && err != http.ErrServerClosed {
				log.Printf("error: server exited: %v", err)
				os.Exit(1)
			}
		}()
	}

	select {}
}

// serveLastMetrics retrieves the last set of metrics served
func serveLastMetrics(worker *forwarder.Worker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		families := worker.LastMetrics()
		w.Header().Set("Content-Type", string(expfmt.FmtText))
		encoder := expfmt.NewEncoder(w, expfmt.FmtText)
		for _, family := range families {
			if family == nil {
				continue
			}
			if err := encoder.Encode(family); err != nil {
				log.Printf("error: unable to write metrics for family: %v", err)
				break
			}
		}
	})
}
