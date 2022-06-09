defmodule Vax.MixProject do
  use Mix.Project

  def project do
    [
      app: :vax,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps(Mix.env()),
      package: package(),
      test_paths: ["test"],
      dialyzer: [ignore_warnings: "dialyzer.ignore-warnings"]
    ] ++ docs()
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # TODO: need to be fixed for release on hex
  defp deps(:publish) do
    [
      {:ecto, "~> 3.7"},
      {:nimble_pool, "~> 0.2.6"}
    ]
  end

  defp deps(_) do
    [
      {:ecto, "~> 3.7"},
      {:antidote_pb_codec, path: "../antidote_pb_codec", override: true},
      {:antidotec_pb, path: "../antidotec_pb"},
      {:nimble_pool, "~> 0.2.6"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false}
    ]
  end

  defp docs do
    [
      name: "Vax",
      description: "Data access library for the Vaxine database platform.",
      source_url: "https://github.com/vaxine-io/vaxine",
      homepage_url: "https://vaxine.io"
    ]
  end

  defp package do
    [
      name: "vax",
      maintainers: ["James Arthur"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/vaxine-io/vax",
        "Vaxine" => "https://vaxine.io"
      }
    ]
  end
end
