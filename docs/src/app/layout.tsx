import "@/app/global.css";
import { RootProvider } from "fumadocs-ui/provider/next";
import { Inter } from "next/font/google";
import Script from "next/script";
import type { ReactNode } from "react";
import type { Metadata } from "next";

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
});

export const metadata: Metadata = {
  title: {
    default: "Rybbit - Privacy-First Web Analytics Platform",
    template: "%s | Rybbit",
  },
  description:
    "Open-source, privacy-focused web analytics platform. Track your website performance without compromising user privacy. Self-hostable alternative to Google Analytics.",
  keywords: [
    "web analytics",
    "privacy analytics",
    "open source analytics",
    "Google Analytics alternative",
    "website tracking",
    "self-hosted analytics",
  ],
  authors: [{ name: "Rybbit Team" }],
  creator: "Rybbit",
  publisher: "Rybbit",
  metadataBase: new URL("https://rybbit.com"),
  openGraph: {
    type: "website",
    locale: "en_US",
    url: "https://rybbit.com",
    siteName: "Rybbit",
    title: "Rybbit - Privacy-First Web Analytics Platform",
    description:
      "Open-source, privacy-focused web analytics platform. Track your website performance without compromising user privacy.",
    images: [
      {
        url: "/opengraph-image.png",
        width: 1200,
        height: 630,
        alt: "Rybbit Analytics Dashboard",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Rybbit - Privacy-First Web Analytics Platform",
    description:
      "Open-source, privacy-focused web analytics platform. Track your website performance without compromising user privacy.",
    images: ["/opengraph-image.png"],
    creator: "@yang_frog",
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      "max-video-preview": -1,
      "max-image-preview": "large",
      "max-snippet": -1,
    },
  },
  verification: {
    google: "",
    yandex: "",
    yahoo: "",
  },
};

const isDev = process.env.NODE_ENV === "development";

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className="dark">
      <Script
        src="https://demo.rybbit.com/api/script.js"
        data-site-id="21"
        strategy="afterInteractive"
        data-session-replay="true"
        data-web-vitals="true"
        data-track-errors="true"
        data-track-outbound="true"
        {...(isDev && {
          "data-api-key": process.env.NEXT_PUBLIC_RYBBIT_API_KEY,
        })}
      />
      <Script id="matomo" strategy="afterInteractive">
        {`
        var _paq = window._paq = window._paq || [];
        /* tracker methods like "setCustomDimension" should be called before "trackPageView" */
        _paq.push(['trackPageView']);
        _paq.push(['enableLinkTracking']);
        (function() {
          var u = "https://rybbit.matomo.cloud/";
          _paq.push(['setTrackerUrl', u + 'matomo.php']);
          _paq.push(['setSiteId', '1']);
          var d = document, g = d.createElement('script'), s = d.getElementsByTagName('script')[0];
          g.async = true;
          g.src = 'https://cdn.matomo.cloud/rybbit.matomo.cloud/matomo.js';
          s.parentNode.insertBefore(g, s);
        })();
      `}
      </Script>
      <Script type="text/javascript" id="mixpanel" strategy="afterInteractive">
        {`(function (e, c) {
          if (!c.__SV) {
            var l, h;
            window.mixpanel = c;
            c._i = [];
            c.init = function (q, r, f) {
              function t(d, a) {
                var g = a.split(".");
                2 == g.length && (d = d[g[0]], a = g[1]);
                d[a] = function () {
                  d.push([a].concat(Array.prototype.slice.call(arguments, 0)))
                }
              }

              var b = c;
              "undefined" !== typeof f ? b = c[f] = [] : f = "mixpanel";
              b.people = b.people || [];
              b.toString = function (d) {
                var a = "mixpanel";
                "mixpanel" !== f && (a += "." + f);
                d || (a += " (stub)");
                return a
              };
              b.people.toString = function () {
                return b.toString(1) + ".people (stub)"
              };
              l = "disable time_event track track_pageview track_links track_forms track_with_groups add_group set_group remove_group register register_once alias unregister identify name_tag set_config reset opt_in_tracking opt_out_tracking has_opted_in_tracking has_opted_out_tracking clear_opt_in_out_tracking start_batch_senders start_session_recording stop_session_recording people.set people.set_once people.unset people.increment people.append people.union people.track_charge people.clear_charges people.delete_user people.remove".split(" ");
              for (h = 0; h < l.length; h++) t(b, l[h]);
              var n = "set set_once union unset remove delete".split(" ");
              b.get_group = function () {
                function d(p) {
                  a[p] = function () {
                    b.push([g, [p].concat(Array.prototype.slice.call(arguments, 0))])
                  }
                }

                for (var a = {}, g = ["get_group"].concat(Array.prototype.slice.call(arguments, 0)), m = 0; m < n.length; m++) d(n[m]);
                return a
              };
              c._i.push([q, r, f])
            };
            c.__SV = 1.2;
            var k = e.createElement("script");
            k.type = "text/javascript";
            k.async = !0;
            k.src = "undefined" !== typeof MIXPANEL_CUSTOM_LIB_URL ? MIXPANEL_CUSTOM_LIB_URL : "file:" ===
            e.location.protocol && "//cdn.mxpnl.com/libs/mixpanel-2-latest.min.js".match(/^\/\//) ? "https://cdn.mxpnl.com/libs/mixpanel-2-latest.min.js" : "//cdn.mxpnl.com/libs/mixpanel-2-latest.min.js";
            e = e.getElementsByTagName("script")[0];
            e.parentNode.insertBefore(k, e)
          }
        })(document, window.mixpanel || [])

          mixpanel.init('5409b6daffa187942af0f05518c2a4eb', {
          autocapture: true,
          record_sessions_percent: 0,
        })`}
      </Script>
      <body className={`flex flex-col min-h-screen ${inter.variable} font-sans`}>
        <RootProvider
          theme={{
            forcedTheme: "dark",
            defaultTheme: "dark",
            enabled: false,
          }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
