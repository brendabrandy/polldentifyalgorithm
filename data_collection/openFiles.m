file = 'uwnd.mon.mean.nc';

ncid = netcdf.open(file,'NOWRITE');
ncdisp(file);
vardata_lat = ncread(file,'lat');
vardata_lon = ncread(file,'lon');
vardata_time = ncread(file,'time');
vardata_wnd = ncread(file,'uwnd');
vardata_lat_length = length(vardata_lat);
vardata_lon_length = length(vardata_lon);
vardata_time_length = length(vardata_time);
total_length = vardata_lat_length*vardata_lon_length*vardata_time_length;
idx = 1;
A = zeros(total_length,4);
for lat = 1:vardata_lat_length
    for lon = 1:vardata_lon_length
        for time = 1:vardata_time_length
            if (vardata_time(time) >= 1753152)
                B = [vardata_lon(lon) vardata_lat(lat) vardata_time(time) vardata_wnd(lon,lat,time)];
                A(idx,:) = B;
                idx = idx + 1;
            end
        end
    end
end
dlmwrite('uwnd_mon_mean.csv',A,'precision',10);