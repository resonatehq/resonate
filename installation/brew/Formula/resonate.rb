class Resonate < Formula
  desc "A dead simple programming model for modern applications"
  homepage "https://www.resonatehq.io/"
  url "https://github.com/resonatehq/resonate/archive/refs/tags/v0.2.0.tar.gz"
  license "Apache-2.0"

  depends_on "go" => :build

  def install
    ENV["GOPATH"] = buildpath
    system "go", "get", "-u", "github.com/resonatehq/resonate"
    system "go", "build", "-o", bin/"resonate", "github.com/resonatehq/resonate"
  end

  test do
    assert_match "Usage:", shell_output("#{bin}/resonatehq --help")
  end
end
