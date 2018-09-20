package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
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
		Listen:       "localhost:9002",
		LimitBytes:   200 * 1024,
		Rules:        []string{`{__name__="up"}`},
		Interval:     4*time.Minute + 30*time.Second,
		N:            1,
		InitialDelay: time.Duration(-1),
	}
	cmd := &cobra.Command{
		Short: "Federate Prometheus via push",

		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return opt.Run()
		},
	}

	cmd.Flags().DurationVar(&opt.InitialDelay, "delay", opt.InitialDelay, "The initial delay before sending metrics.")
	cmd.Flags().IntVar(&opt.N, "number", opt.N, "Number of workers to spawn.")
	cmd.Flags().StringVar(&opt.Listen, "listen", opt.Listen, "A host:port to listen on for health and metrics.")
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

	To          string
	ToUpload    string
	ToAuthorize string
	ToToken     string
	ToTokenFile string

	RenameFlag []string
	Renames    map[string]string

	AnonymizeLabels   []string
	AnonymizeSalt     string
	AnonymizeSaltFile string

	Rules     []string
	RulesFile string

	LabelFlag []string
	Labels    map[string]string

	Interval     time.Duration
	InitialDelay time.Duration

	N int
}

type transforms struct {
	labelRetriever  metricfamily.LabelRetriever
	labels          map[string]string
	anonymizeLabels []string
	anonymizeSalt   string
	renames         map[string]string
	rules           []string

	forwarder forwarder.Worker
}

func (t *transforms) Transforms() []metricfamily.Transformer {
	var transforms metricfamily.AllTransformer
	if len(t.labels) > 0 || t.labelRetriever != nil {
		transforms = append(transforms, metricfamily.NewLabel(t.labels, t.labelRetriever))
	}
	if len(t.anonymizeLabels) > 0 {
		transforms = append(transforms, metricfamily.NewMetricsAnonymizer(t.anonymizeSalt, t.anonymizeLabels, nil))
	}
	if len(t.renames) > 0 {
		transforms = append(transforms, metricfamily.RenameMetrics{Names: t.renames})
	}
	transforms = append(transforms,
		metricfamily.NewDropInvalidFederateSamples(time.Now().Add(-24*time.Hour)),
		metricfamily.TransformerFunc(metricfamily.PackMetrics),
		metricfamily.TransformerFunc(metricfamily.SortMetrics),
	)
	return []metricfamily.Transformer{transforms}
}

func (t *transforms) MatchRules() []string {
	return t.rules
}

func (o *Options) Run() error {
	if len(o.ToToken) == 0 && len(o.ToTokenFile) > 0 {
		data, err := ioutil.ReadFile(o.ToTokenFile)
		if err != nil {
			return fmt.Errorf("unable to read --to-token-file: %v", err)
		}
		o.ToToken = strings.TrimSpace(string(data))
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

	var ws []forwarder.Worker
	for i := 0; i < o.N; i++ {
		c, u, lt, err := o.clientAndURL(i)

		ts := transforms{
			labelRetriever:  lt,
			labels:          o.Labels,
			anonymizeLabels: o.AnonymizeLabels,
			anonymizeSalt:   o.AnonymizeSalt,
			renames:         o.Renames,
			rules:           o.Rules,
		}

		if err != nil {
			return fmt.Errorf("failed to generate HTTP client and URL for worker %d: %v", i, err)
		}
		worker := forwarder.New(url.URL{}, u, &ts)
		worker.ToClient = metricsclient.New(c, o.LimitBytes, o.Interval, "federate_to")
		worker.FromClient = metricsclient.NewMock()
		worker.Interval = o.Interval
		ws = append(ws, *worker)

		go func(i int) {
			initialDelay := o.InitialDelay
			if initialDelay < 0 {
				initialDelay = time.Duration(rand.Intn(int(worker.Interval)))
			}
			log.Printf("Starting telemeter-client %d, sending metrics in %v", i, initialDelay)
			time.Sleep(initialDelay)
			worker.Run()
		}(i)
	}

	if len(o.Listen) > 0 {
		handlers := http.NewServeMux()
		telemeterhttp.AddDebug(handlers)
		telemeterhttp.AddHealth(handlers)
		telemeterhttp.AddMetrics(handlers)
		go func() {
			if err := http.ListenAndServe(o.Listen, handlers); err != nil && err != http.ErrServerClosed {
				log.Printf("error: server exited: %v", err)
				os.Exit(1)
			}
		}()
	}

	select {}
}

func (o *Options) clientAndURL(id int) (*http.Client, *url.URL, metricfamily.LabelRetriever, error) {
	var to, toUpload, toAuthorize *url.URL
	var err error
	if len(o.ToUpload) > 0 {
		to, err = url.Parse(o.ToUpload)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("--to is not a valid URL: %v", err)
		}
	}
	if len(o.ToAuthorize) > 0 {
		toAuthorize, err = url.Parse(o.ToAuthorize)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("--to-auth is not a valid URL: %v", err)
		}
	}
	if len(o.To) > 0 {
		to, err = url.Parse(o.To)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("--to is not a valid URL: %v", err)
		}
		if len(to.Path) == 0 {
			to.Path = "/"
		}
		if toAuthorize == nil {
			u := *to
			u.Path = path.Join(to.Path, "authorize")
			q := to.Query()
			q.Add("id", strconv.Itoa(id))
			u.RawQuery = q.Encode()
			toAuthorize = &u
		}
		if toUpload == nil {
			u := *to
			u.Path = path.Join(to.Path, "upload")
			toUpload = &u
		}
	}

	if toUpload == nil || toAuthorize == nil {
		return nil, nil, nil, fmt.Errorf("either --to or --to-auth and --to-upload must be specified")
	}

	var lt metricfamily.LabelRetriever
	toClient := &http.Client{Transport: metricsclient.DefaultTransport()}
	if len(o.ToToken) > 0 {
		// exchange our token for a token from the authorize endpoint, which also gives us a
		// set of expected labels we must include
		rt := remote.NewServerRotatingRoundTripper(o.ToToken, toAuthorize, toClient.Transport)
		lt = rt
		toClient.Transport = rt
	}

	return toClient, toUpload, lt, nil
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
