class Resonate < Formula
  desc "A dead simple programming model for modern applications"
  homepage "https://www.resonatehq.io/"
  license "Apache-2.0"

  depends_on "go" => :build

  # Fetch the latest release URL from GitHub Releases
  latest_release_url = JSON.parse(`curl -s https://api.github.com/repos/resonatehq/resonate/releases/latest`)["tarball_url"]

  # Use the latest release URL in the formula
  url latest_release_url

  def install
    ENV["GOPATH"] = buildpath
    system "go", "get", "-u", "github.com/resonatehq/resonate"
    system "go", "build", "-o", bin/"resonate", "github.com/resonatehq/resonate"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/resonatehq --help")
  end
end
