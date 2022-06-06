unless System.get_env("INTEGRATION") do
  ExUnit.configure(exclude: :integration)
end

ExUnit.start(capture_log: true)
