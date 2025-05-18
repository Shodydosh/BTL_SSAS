-- Drop tables in correct order (child tables first, then parent tables)
IF OBJECT_ID('mat_hang_duoc_dat', 'U') IS NOT NULL DROP TABLE mat_hang_duoc_dat;
IF OBJECT_ID('don_dat_hang', 'U') IS NOT NULL DROP TABLE don_dat_hang;
IF OBJECT_ID('khach_hang_du_lich', 'U') IS NOT NULL DROP TABLE khach_hang_du_lich;
IF OBJECT_ID('khach_hang_buu_dien', 'U') IS NOT NULL DROP TABLE khach_hang_buu_dien;
IF OBJECT_ID('khach_hang', 'U') IS NOT NULL DROP TABLE khach_hang;
IF OBJECT_ID('mat_hang_luu_tru', 'U') IS NOT NULL DROP TABLE mat_hang_luu_tru;
IF OBJECT_ID('mat_hang', 'U') IS NOT NULL DROP TABLE mat_hang;
IF OBJECT_ID('cua_hang', 'U') IS NOT NULL DROP TABLE cua_hang;
IF OBJECT_ID('thanh_pho', 'U') IS NOT NULL DROP TABLE thanh_pho;
GO

create table thanh_pho (
   mathanhpho     varchar(20) primary key,
   tenthanhpho    varchar(100),
   diachivp       varchar(500),
   bang           varchar(50),
   ngaythanhlapvp date
);

create table cua_hang (
   macuahang    varchar(20) primary key,
   mathanhpho   varchar(20),
   sodienthoai  varchar(20),
   ngaythanhlap date,
   foreign key ( mathanhpho )
      references thanh_pho ( mathanhpho )
);

create table mat_hang (
   mamh          varchar(20) primary key,
   mota          varchar(1000),
   kichco        float,
   trongluong    float,
   gia           float,
   ngaybatdauban date
);

create table mat_hang_luu_tru (
   macuahang        varchar(20),
   mamh             varchar(20),
   soluong          int,
   thoigiannhapxuat datetime,
   primary key ( macuahang,
                 mamh,
                 thoigiannhapxuat ),
   foreign key ( macuahang )
      references cua_hang ( macuahang ),
   foreign key ( mamh )
      references mat_hang ( mamh )
);

create table khach_hang (
   makh               varchar(20) primary key,
   tenkh              varchar(100),
   mathanhpho         varchar(20),
   ngaydathangdautien date,
   foreign key ( mathanhpho )
      references thanh_pho ( mathanhpho )
);

create table khach_hang_buu_dien (
   makh          varchar(20) primary key,
   diachibuudien varchar(500),
   foreign key ( makh )
      references khach_hang ( makh )
);

create table khach_hang_du_lich (
   makh         varchar(20) primary key,
   huongdanvien varchar(100),
   foreign key ( makh )
      references khach_hang ( makh )
);

create table don_dat_hang (
   madon           varchar(20) primary key,
   thoigiandathang datetime,
   makh            varchar(20),
   foreign key ( makh )
      references khach_hang ( makh )
);

create table mat_hang_duoc_dat (
   madon       varchar(20),
   mamh        varchar(20),
   soluong     int,
   giadat      float,
   thoigiandat datetime,
   primary key ( madon,
                 mamh ),
   foreign key ( madon )
      references don_dat_hang ( madon ),
   foreign key ( mamh )
      references mat_hang ( mamh )
);