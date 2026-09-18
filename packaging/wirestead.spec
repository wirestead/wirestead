Name:           wirestead
Version:        0.9.6
Release:        0
Summary:        Async serial, TCP, UDP and Unix socket communication library
License:        Apache-2.0
Group:          Development/Libraries
URL:            https://github.com/wirestead/wirestead
Source0:        %{name}-%{version}.tar.gz

# The three version floors below are duplicated from
# cmake/WiresteadDependencies.cmake and CMakeLists.txt. RPM cannot read them
# from CMake, so they drift silently if only one side is bumped;
# docs/release_checklist.md carries a box for keeping them together.
BuildRequires:  cmake >= 3.12
BuildRequires:  gcc-c++
BuildRequires:  pkgconfig
BuildRequires:  boost-devel >= 1.74
BuildRequires:  spdlog-devel >= 1.8

%description
Wirestead provides one asynchronous API over serial ports, TCP, UDP and Unix
domain sockets, so an application can move between transports without being
rewritten. It adds automatic reconnect, backpressure policies, message framing
and runtime counters on top of Boost.Asio.

This package contains the shared library.

%package devel
Summary:        Development files for %{name}
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}
Requires:       boost-devel >= 1.74
Requires:       spdlog-devel >= 1.8

%description devel
Headers, CMake package configuration and pkg-config files for %{name}.

Boost and spdlog are required here rather than by the shared library: the
installed headers include boost/asio and spdlog headers, so anything compiling
against Wirestead needs them, while libwirestead.so itself links neither Boost
component (Asio and Boost.System are header-only since Boost 1.69).

%prep
%setup -q

%build
# Plain cmake rather than the %%cmake macro, which differs between RPM
# distributions - Fedora's builds out of tree under a fixed directory, others
# do not have it at all.
# RelWithDebInfo rather than Release: rpmbuild extracts a debuginfo subpackage
# by default, and a build with no -g leaves its file list empty, which is a hard
# error rather than a skip.
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=%{_prefix} \
    -DCMAKE_INSTALL_LIBDIR=%{_lib} \
    -DWIRESTEAD_BUILD_SHARED=ON \
    -DWIRESTEAD_BUILD_STATIC=OFF \
    -DWIRESTEAD_BUILD_TESTS=OFF \
    -DWIRESTEAD_ENABLE_INSTALL=ON \
    -DWIRESTEAD_ENABLE_PKGCONFIG=ON
cmake --build build %{?_smp_mflags}

%install
DESTDIR=%{buildroot} cmake --install build

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
# The build installs these into CMAKE_INSTALL_DOCDIR itself, so they are claimed
# from there rather than copied again out of the source tree.
%license %{_docdir}/%{name}/LICENSE
%license %{_docdir}/%{name}/NOTICE
%doc %{_docdir}/%{name}/README.md
%{_libdir}/libwirestead.so.*

%files devel
%{_includedir}/wirestead/
%{_includedir}/wirestead_export.hpp
%{_libdir}/libwirestead.so
%{_libdir}/cmake/wirestead/
%{_libdir}/pkgconfig/wirestead.pc
%{_datadir}/wirestead/package.xml

%changelog
