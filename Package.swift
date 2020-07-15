// swift-tools-version:4.2

import PackageDescription

let package = Package(
	name: "PerfectPostresql-Demo",
	products: [
		.executable(name: "PerfectPostresql-Demo", targets: ["PerfectPostresql-Demo"])
	],
	dependencies: [
		.package(url: "https://github.com/PerfectlySoft/Perfect-HTTPServer.git", from: "3.0.0"),
        .package(url: "https://github.com/PerfectlySoft/Perfect-PostgreSQL.git", from: "3.0.0")
	],
	targets: [
		.target(name: "PerfectPostresql-Demo", dependencies: ["PerfectHTTPServer", "PerfectPostgreSQL"])
	]
)
