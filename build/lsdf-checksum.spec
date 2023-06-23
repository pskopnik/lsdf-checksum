%global upstream_version 0.4.0

Name:               lsdf-checksum
Version:            %( echo %upstream_version | sed -E 's/-(beta|alpha|rc)/~\1/i' | tr '-' '.' )
Release:            1%{?dist}
Summary:            Checksumming tool for GPFS filesystems
License:            MIT
URL:                https://git.scc.kit.edu/sdm/online-storage/lsdf-checksum
Source0:            lsdf-checksum-%{upstream_version}.tar.gz

%global debug_package %{nil}

%description
Checksum Tool for GPFS file systems

%package master
Summary:            Master component of %{name}

%description master
Master component of %{name}.
It's the coordinator which performs all central logic for performing
checksumming runs, primarily interacting with GPFS management APIs,
managing persistent and data and communicating with workers via a
task queue.

%package worker
Summary:            Worker component of %{name}

%description worker
Worker component of %{name}.
It's responsible for reading files and computing their checksums and
communicates with the master via a task queue.


%clean


%prep
%autosetup -p1 -n lsdf-checksum-%{upstream_version}

%build
mkdir -p man
./lsdf-checksum-master --help-man > man/lsdf-checksum-master.1
./lsdf-checksum-worker --help-man > man/lsdf-checksum-worker.1

%install
install -m 755 -D lsdf-checksum-master %{buildroot}/%{_bindir}/lsdf-checksum-master
install -m 644 -D man/lsdf-checksum-master.1 %{buildroot}/%{_mandir}/man1/lsdf-checksum-master.1
install -m 755 -D lsdf-checksum-worker %{buildroot}/%{_bindir}/lsdf-checksum-worker
install -m 644 -D man/lsdf-checksum-worker.1 %{buildroot}/%{_mandir}/man1/lsdf-checksum-worker.1

%if 0%{?rhel} == 7
%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%else

%ldconfig_scriptlets

%endif

%files

%files master
%{_bindir}/lsdf-checksum-master
%{_mandir}/man1/lsdf-checksum-master.1*

%files worker
%{_bindir}/lsdf-checksum-worker
%{_mandir}/man1/lsdf-checksum-worker.1*


# The changelog must contain the version in the proper RPM E-V-R notation.
%changelog
* Fri Jun 23 2023 Paul Skopnik <paul.skopnik@kit.edu> - 0.4.0
- Release v0.4.0
