-- Database sẽ được sử dụng từ config (MYSQL_DATABASE trong .env)
-- Nếu cần tạo database mới, uncomment dòng dưới:
-- CREATE DATABASE IF NOT EXISTS defaultdb;

-- USE sẽ được thực hiện tự động bởi MySQLConnector dựa trên config

-- ============================================
-- Table: doi_bong
-- Mô tả: Thông tin các đội bóng
-- ============================================
CREATE TABLE IF NOT EXISTS doi_bong (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    ten_doi VARCHAR(255) NOT NULL UNIQUE COMMENT 'Tên đội bóng',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    INDEX idx_ten_doi (ten_doi) COMMENT 'Index theo tên đội'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: mua_giai
-- Mô tả: Thông tin các mùa giải
-- ============================================
CREATE TABLE IF NOT EXISTS mua_giai (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    ten_mua_giai VARCHAR(20) NOT NULL UNIQUE COMMENT 'Tên mùa giải (ví dụ: 2024-25)',
    nam_bat_dau INT COMMENT 'Năm bắt đầu (ví dụ: 2024)',
    nam_ket_thuc INT GENERATED ALWAYS AS (nam_bat_dau + 1) STORED COMMENT 'Năm kết thúc (Tự động = nam_bat_dau + 1)',
    ngay_bat_dau DATE COMMENT 'Ngày bắt đầu mùa giải',
    ngay_ket_thuc DATE COMMENT 'Ngày kết thúc mùa giải',
    so_doi INT COMMENT 'Số đội tham gia',
    so_vong_dau INT COMMENT 'Tổng số vòng đấu',
    thong_tin_bo_sung JSON COMMENT 'Thông tin bổ sung dạng JSON',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    INDEX idx_ten_mua_giai (ten_mua_giai) COMMENT 'Index theo tên mùa giải',
    INDEX idx_nam_bat_dau (nam_bat_dau) COMMENT 'Index theo năm bắt đầu'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: mua_giai_doi_bong
-- Mô tả: Quan hệ many-to-many giữa mùa giải và đội bóng
-- Mục đích: Xem mùa giải đó có những đội bóng nào
-- ============================================
CREATE TABLE IF NOT EXISTS mua_giai_doi_bong (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_mua_giai BIGINT NOT NULL COMMENT 'ID mùa giải (FK)',
    id_doi_bong BIGINT NOT NULL COMMENT 'ID đội bóng (FK)',
    vi_tri_bang_xep_hang INT COMMENT 'Vị trí trên bảng xếp hạng',
    so_tran_thang INT DEFAULT 0 COMMENT 'Số trận thắng',
    so_tran_hoa INT DEFAULT 0 COMMENT 'Số trận hòa',
    so_tran_thua INT DEFAULT 0 COMMENT 'Số trận thua',
    tong_ban_thang INT DEFAULT 0 COMMENT 'Tổng bàn thắng',
    tong_ban_thua INT DEFAULT 0 COMMENT 'Tổng bàn thua',
    hieu_so INT DEFAULT 0 COMMENT 'Hiệu số bàn thắng',
    tong_diem INT DEFAULT 0 COMMENT 'Tổng điểm',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_mua_giai) REFERENCES mua_giai(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_bong) REFERENCES doi_bong(id) ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_mua_giai (id_mua_giai) COMMENT 'Index theo ID mùa giải',
    INDEX idx_id_doi_bong (id_doi_bong) COMMENT 'Index theo ID đội bóng',
    INDEX idx_vi_tri_bang_xep_hang (vi_tri_bang_xep_hang) COMMENT 'Index theo vị trí bảng xếp hạng',
    UNIQUE KEY unique_mua_giai_doi_bong (id_mua_giai, id_doi_bong) COMMENT 'Ràng buộc duy nhất: mỗi đội chỉ có 1 bản ghi trong mỗi mùa giải'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: vong_dau
-- Mô tả: Thông tin các vòng đấu trong mùa giải
-- Quan hệ: FK đến mua_giai
-- ============================================
CREATE TABLE IF NOT EXISTS vong_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_mua_giai BIGINT NOT NULL COMMENT 'ID mùa giải (FK)',
    so_vong_dau INT NOT NULL COMMENT 'Số vòng đấu (1, 2, 3, ...)',
    so_tran_dau INT DEFAULT 0 COMMENT 'Số trận đấu trong vòng này',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Key
    FOREIGN KEY (id_mua_giai) REFERENCES mua_giai(id) ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_mua_giai (id_mua_giai) COMMENT 'Index theo ID mùa giải',
    INDEX idx_so_vong_dau (so_vong_dau) COMMENT 'Index theo số vòng đấu',
    UNIQUE KEY unique_mua_giai_vong_dau (id_mua_giai, so_vong_dau) COMMENT 'Ràng buộc duy nhất: mỗi vòng đấu chỉ có 1 bản ghi trong mỗi mùa giải'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: tran_dau
-- Mô tả: Thông tin cơ bản về trận đấu
-- Quan hệ: FK đến vong_dau, doi_bong (đội nhà và đội khách)
-- ============================================
CREATE TABLE IF NOT EXISTS tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_tran_dau INT NOT NULL UNIQUE COMMENT 'ID trận đấu (từ hệ thống Premier League)',
    id_vong_dau BIGINT NOT NULL COMMENT 'ID vòng đấu (FK)',
    id_doi_nha BIGINT NOT NULL COMMENT 'ID đội nhà (FK)',
    id_doi_khach BIGINT NOT NULL COMMENT 'ID đội khách (FK)',
    ngay_gio DATETIME COMMENT 'Ngày giờ trận đấu',
    san_van_dong VARCHAR(255) COMMENT 'Sân vận động',
    trong_tai VARCHAR(255) COMMENT 'Trọng tài',
    url VARCHAR(500) COMMENT 'URL trang web trận đấu',
    thoi_gian_scrape TIMESTAMP COMMENT 'Thời gian scrape dữ liệu',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_vong_dau) REFERENCES vong_dau(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_nha) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_khach) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_tran_dau (id_tran_dau) COMMENT 'Index theo ID trận đấu',
    INDEX idx_id_vong_dau (id_vong_dau) COMMENT 'Index theo ID vòng đấu',
    INDEX idx_id_doi_nha (id_doi_nha) COMMENT 'Index theo ID đội nhà',
    INDEX idx_id_doi_khach (id_doi_khach) COMMENT 'Index theo ID đội khách',
    INDEX idx_ngay_gio (ngay_gio) COMMENT 'Index theo ngày giờ'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: ket_qua_tran_dau
-- Mô tả: Kết quả trận đấu của từng đội
-- Quan hệ: FK đến tran_dau và doi_bong
-- ============================================
CREATE TABLE IF NOT EXISTS ket_qua_tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_tran_dau BIGINT NOT NULL COMMENT 'ID trận đấu (FK)',
    id_doi BIGINT NOT NULL COMMENT 'ID đội (FK)',
    id_doi_thu BIGINT NOT NULL COMMENT 'ID đối thủ (FK)',
    la_doi_nha BOOLEAN NOT NULL COMMENT 'Có phải đội nhà không (1=nhà, 0=khách)',
    ket_qua CHAR(1) COMMENT 'Kết quả: W=Thắng, L=Thua, D=Hòa',
    ban_thang_ghi_duoc INT COMMENT 'Số bàn thắng ghi được',
    ban_thua INT COMMENT 'Số bàn thua',
    tong_diem INT COMMENT 'Tổng điểm',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_tran_dau) REFERENCES tran_dau(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_doi) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_thu) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_tran_dau (id_tran_dau) COMMENT 'Index theo ID trận đấu',
    INDEX idx_id_doi (id_doi) COMMENT 'Index theo ID đội',
    INDEX idx_id_doi_thu (id_doi_thu) COMMENT 'Index theo ID đối thủ',
    INDEX idx_ket_qua (ket_qua) COMMENT 'Index theo kết quả',
    INDEX idx_la_doi_nha (la_doi_nha) COMMENT 'Index theo đội nhà/khách',
    UNIQUE KEY unique_match_team (id_tran_dau, id_doi) COMMENT 'Ràng buộc duy nhất: mỗi đội chỉ có 1 bản ghi cho mỗi trận đấu'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: thong_ke_tran_dau
-- Mô tả: Thống kê chi tiết của từng đội trong trận đấu
-- Quan hệ: FK đến ket_qua_tran_dau
-- Lưu tất cả các thống kê từ matches.csv và analytics/stats
-- ============================================
CREATE TABLE IF NOT EXISTS thong_ke_tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_ket_qua BIGINT NOT NULL COMMENT 'ID kết quả trận đấu (FK)',
    
    -- Thống kê cơ bản
    ty_le_so_huu_banh DECIMAL(5,2) COMMENT 'Tỷ lệ sở hữu bóng (%)',
    tong_so_cu_sut INT COMMENT 'Tổng số cú sút',
    cu_sut_trung_dich INT COMMENT 'Cú sút trúng đích',
    cu_sut_ngoai_dich INT COMMENT 'Cú sút ngoài đích',
    cu_sut_trong_vong_cam INT COMMENT 'Cú sút trong vòng cấm',
    cu_sut_ngoai_vong_cam INT COMMENT 'Cú sút ngoài vòng cấm',
    
    -- Thống kê đường chuyền
    tong_so_duong_chuyen INT COMMENT 'Tổng số đường chuyền',
    ty_le_duong_chuyen_thanh_cong DECIMAL(5,2) COMMENT 'Tỷ lệ đường chuyền thành công (%)',
    so_duong_chuyen_dai_thanh_cong INT COMMENT 'Số đường chuyền dài thành công',
    ty_le_duong_chuyen_dai_thanh_cong DECIMAL(5,2) COMMENT 'Tỷ lệ đường chuyền dài thành công (%)',
    so_duong_chuyen_ngang_thanh_cong INT COMMENT 'Số đường chuyền ngang thành công',
    ty_le_duong_chuyen_ngang_thanh_cong DECIMAL(5,2) COMMENT 'Tỷ lệ đường chuyền ngang thành công (%)',
    so_duong_chuyen_xuyen_phong INT COMMENT 'Số đường chuyền xuyên phòng',
    ty_le_duong_chuyen_xuyen_phong DECIMAL(5,2) COMMENT 'Tỷ lệ đường chuyền xuyên phòng (%)',
    
    -- Thống kê pha bóng
    tong_so_pha_re_bong INT COMMENT 'Tổng số pha rê bóng',
    pha_bong_thanh_cong INT COMMENT 'Pha bóng thành công',
    so_lan_chan_bong INT COMMENT 'Số lần chạm bóng',
    so_lan_chan_bong_trong_vong_cam_doi_thu INT COMMENT 'Số lần chạm bóng trong vòng cấm đối thủ',
    
    -- Thống kê phòng thủ
    pha_bong_thang INT COMMENT 'Pha bóng thắng',
    ty_le_pha_bong_thang DECIMAL(5,2) COMMENT 'Tỷ lệ pha bóng thắng (%)',
    tran_chap_thang INT COMMENT 'Tranh chấp thắng',
    tran_chap_khong_trung_thang INT COMMENT 'Tranh chấp không trung thắng',
    chan_bong INT COMMENT 'Chặn bóng',
    pha_bong_ra INT COMMENT 'Phá bóng ra',
    chan_cu_sut INT COMMENT 'Chặn cú sút',
    cu_cuu INT COMMENT 'Cứu thua',
    
    -- Thống kê khác
    so_phat_goc INT COMMENT 'Số phát góc',
    so_vi_vi INT COMMENT 'Số việt vị',
    so_loi_pham INT COMMENT 'Số lỗi phạm',
    so_the_vang INT COMMENT 'Số thẻ vàng',
    so_the_do INT COMMENT 'Số thẻ đỏ',
    co_hoi_lon INT COMMENT 'Cơ hội lớn',
    co_hoi_lon_tao_ra INT COMMENT 'Cơ hội lớn tạo ra',
    danh_trung_khung_thanh INT COMMENT 'Đánh trúng khung thành',
    xg DECIMAL(6,2) COMMENT 'Expected Goals (xG)',
    quang_duong_di_chuyen DECIMAL(6,2) COMMENT 'Quãng đường di chuyển (km)',
    
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Key
    FOREIGN KEY (id_ket_qua) REFERENCES ket_qua_tran_dau(id) ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_ket_qua (id_ket_qua) COMMENT 'Index theo ID kết quả',
    UNIQUE KEY unique_ket_qua (id_ket_qua) COMMENT 'Ràng buộc duy nhất: mỗi kết quả chỉ có 1 bản ghi thống kê'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: cau_thu
-- Mô tả: Thông tin các cầu thủ
-- ============================================
CREATE TABLE IF NOT EXISTS cau_thu (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    ten_cau_thu VARCHAR(255) NOT NULL COMMENT 'Tên cầu thủ',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    INDEX idx_ten_cau_thu (ten_cau_thu) COMMENT 'Index theo tên cầu thủ'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: cau_thu_tran_dau
-- Mô tả: Quan hệ giữa cầu thủ và trận đấu
-- Dữ liệu từ: players.csv
-- Quan hệ: FK đến tran_dau, doi_bong, cau_thu
-- ============================================
CREATE TABLE IF NOT EXISTS cau_thu_tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_tran_dau BIGINT NOT NULL COMMENT 'ID trận đấu (FK)',
    id_doi_bong BIGINT NOT NULL COMMENT 'ID đội bóng (FK)',
    id_cau_thu BIGINT COMMENT 'ID cầu thủ (FK) - NULL nếu cầu thủ chưa có trong bảng cau_thu',
    ten_cau_thu VARCHAR(255) NOT NULL COMMENT 'Tên cầu thủ (từ CSV)',
    vi_tri VARCHAR(50) COMMENT 'Vị trí thi đấu trong trận này',
    la_doi_hinh_xuat_phat BOOLEAN DEFAULT FALSE COMMENT 'Có trong đội hình xuất phát không',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_tran_dau) REFERENCES tran_dau(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_bong) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (id_cau_thu) REFERENCES cau_thu(id) ON DELETE SET NULL ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_tran_dau (id_tran_dau) COMMENT 'Index theo ID trận đấu',
    INDEX idx_id_doi_bong (id_doi_bong) COMMENT 'Index theo ID đội bóng',
    INDEX idx_id_cau_thu (id_cau_thu) COMMENT 'Index theo ID cầu thủ',
    INDEX idx_ten_cau_thu (ten_cau_thu) COMMENT 'Index theo tên cầu thủ',
    INDEX idx_vi_tri (vi_tri) COMMENT 'Index theo vị trí'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: su_kien_tran_dau
-- Mô tả: Các sự kiện trong trận đấu (ghi bàn, thẻ, thay người, v.v.)
-- Dữ liệu từ: events.csv
-- Quan hệ: FK đến tran_dau, doi_bong, cau_thu
-- ============================================
CREATE TABLE IF NOT EXISTS su_kien_tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_tran_dau BIGINT NOT NULL COMMENT 'ID trận đấu (FK)',
    id_doi_bong BIGINT NOT NULL COMMENT 'ID đội bóng (FK)',
    id_cau_thu BIGINT COMMENT 'ID cầu thủ (FK) - NULL nếu cầu thủ chưa có trong bảng cau_thu',
    ten_cau_thu VARCHAR(255) COMMENT 'Tên cầu thủ (từ CSV)',
    loai_su_kien VARCHAR(50) NOT NULL COMMENT 'Loại sự kiện: Goal, Card, Substitution, etc.',
    phut INT COMMENT 'Phút diễn ra sự kiện',
    chi_tiet TEXT COMMENT 'Chi tiết sự kiện (JSON hoặc text)',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_tran_dau) REFERENCES tran_dau(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_bong) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (id_cau_thu) REFERENCES cau_thu(id) ON DELETE SET NULL ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_tran_dau (id_tran_dau) COMMENT 'Index theo ID trận đấu',
    INDEX idx_id_doi_bong (id_doi_bong) COMMENT 'Index theo ID đội bóng',
    INDEX idx_id_cau_thu (id_cau_thu) COMMENT 'Index theo ID cầu thủ',
    INDEX idx_loai_su_kien (loai_su_kien) COMMENT 'Index theo loại sự kiện',
    INDEX idx_phut (phut) COMMENT 'Index theo phút'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: doi_hinh_tran_dau
-- Mô tả: Đội hình của từng đội trong trận đấu
-- Dữ liệu từ: matches.csv (home_formation, away_formation, home_starting_xi, away_starting_xi)
-- Quan hệ: FK đến tran_dau, doi_bong
-- ============================================
CREATE TABLE IF NOT EXISTS doi_hinh_tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_tran_dau BIGINT NOT NULL COMMENT 'ID trận đấu (FK)',
    id_doi_bong BIGINT NOT NULL COMMENT 'ID đội bóng (FK)',
    doi_hinh VARCHAR(20) COMMENT 'Đội hình (ví dụ: 4-3-3)',
    vi_tri_bang_xep_hang INT COMMENT 'Vị trí trên bảng xếp hạng trước trận đấu',
    doi_hinh_xuat_phat TEXT COMMENT 'Đội hình xuất phát (JSON hoặc text)',
    cau_thu_du_bi TEXT COMMENT 'Cầu thủ dự bị (JSON hoặc text)',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_tran_dau) REFERENCES tran_dau(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_bong) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_tran_dau (id_tran_dau) COMMENT 'Index theo ID trận đấu',
    INDEX idx_id_doi_bong (id_doi_bong) COMMENT 'Index theo ID đội bóng',
    UNIQUE KEY unique_tran_dau_doi_bong (id_tran_dau, id_doi_bong) COMMENT 'Ràng buộc duy nhất: mỗi đội chỉ có 1 bản ghi đội hình cho mỗi trận đấu'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Table: ban_thang_tran_dau
-- Mô tả: Chi tiết các bàn thắng trong trận đấu
-- Dữ liệu từ: matches.csv (home_goal_scorers, away_goal_scorers, home_goal_assists, away_goal_assists)
-- Quan hệ: FK đến tran_dau, doi_bong, cau_thu (người ghi bàn và kiến tạo)
-- ============================================
CREATE TABLE IF NOT EXISTS ban_thang_tran_dau (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID tự động tăng',
    id_tran_dau BIGINT NOT NULL COMMENT 'ID trận đấu (FK)',
    id_doi_bong BIGINT NOT NULL COMMENT 'ID đội bóng ghi bàn (FK)',
    id_cau_thu_ghi_ban BIGINT COMMENT 'ID cầu thủ ghi bàn (FK)',
    ten_cau_thu_ghi_ban VARCHAR(255) COMMENT 'Tên cầu thủ ghi bàn (từ CSV)',
    id_cau_thu_kien_tao BIGINT COMMENT 'ID cầu thủ kiến tạo (FK)',
    ten_cau_thu_kien_tao VARCHAR(255) COMMENT 'Tên cầu thủ kiến tạo (từ CSV)',
    phut INT COMMENT 'Phút ghi bàn',
    loai_ban_thang VARCHAR(50) COMMENT 'Loại bàn thắng: Normal, Penalty, Own Goal, etc.',
    thoi_gian_tao TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo',
    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật',
    
    -- Foreign Keys
    FOREIGN KEY (id_tran_dau) REFERENCES tran_dau(id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_doi_bong) REFERENCES doi_bong(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    FOREIGN KEY (id_cau_thu_ghi_ban) REFERENCES cau_thu(id) ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (id_cau_thu_kien_tao) REFERENCES cau_thu(id) ON DELETE SET NULL ON UPDATE CASCADE,
    
    -- Indexes
    INDEX idx_id_tran_dau (id_tran_dau) COMMENT 'Index theo ID trận đấu',
    INDEX idx_id_doi_bong (id_doi_bong) COMMENT 'Index theo ID đội bóng',
    INDEX idx_id_cau_thu_ghi_ban (id_cau_thu_ghi_ban) COMMENT 'Index theo ID cầu thủ ghi bàn',
    INDEX idx_phut (phut) COMMENT 'Index theo phút'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Views cho Phân tích
-- ============================================

-- View: Tóm tắt trận đấu với JOIN các bảng
CREATE OR REPLACE VIEW v_tom_tat_tran_dau AS
SELECT 
    td.id_tran_dau AS 'ID Trận đấu',
    mg.ten_mua_giai AS 'Mùa giải',
    vd.so_vong_dau AS 'Vòng đấu',
    td.ngay_gio AS 'Ngày giờ',
    td.san_van_dong AS 'Sân vận động',
    db1.ten_doi AS 'Đội nhà',
    db2.ten_doi AS 'Đội khách',
    kq.ket_qua AS 'Kết quả',
    kq.ban_thang_ghi_duoc AS 'Bàn thắng',
    kq.ban_thua AS 'Bàn thua',
    tk.ty_le_so_huu_banh AS 'Kiểm soát bóng (%)',
    tk.tong_so_cu_sut AS 'Tổng cú sút',
    tk.cu_sut_trung_dich AS 'Sút trúng đích',
    tk.xg AS 'Expected Goals',
    tk.tong_so_duong_chuyen AS 'Tổng đường chuyền',
    tk.ty_le_duong_chuyen_thanh_cong AS 'Tỷ lệ chuyền thành công (%)',
    kq.thoi_gian_tao AS 'Thời gian tạo'
FROM ket_qua_tran_dau kq
INNER JOIN tran_dau td ON kq.id_tran_dau = td.id
INNER JOIN vong_dau vd ON td.id_vong_dau = vd.id
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
INNER JOIN doi_bong db1 ON kq.id_doi = db1.id
INNER JOIN doi_bong db2 ON kq.id_doi_thu = db2.id
LEFT JOIN thong_ke_tran_dau tk ON kq.id = tk.id_ket_qua
ORDER BY td.ngay_gio DESC, td.id_tran_dau DESC;

-- View: Các trận đấu gần đây
CREATE OR REPLACE VIEW v_tran_dau_gan_day AS
SELECT 
    td.id_tran_dau AS 'ID Trận đấu',
    mg.ten_mua_giai AS 'Mùa giải',
    vd.so_vong_dau AS 'Vòng đấu',
    td.ngay_gio AS 'Ngày giờ',
    db1.ten_doi AS 'Đội nhà',
    db2.ten_doi AS 'Đội khách',
    kq.ket_qua AS 'Kết quả',
    kq.ban_thang_ghi_duoc AS 'Bàn thắng',
    kq.ban_thua AS 'Bàn thua',
    tk.ty_le_so_huu_banh AS 'Kiểm soát bóng (%)',
    tk.xg AS 'Expected Goals'
FROM ket_qua_tran_dau kq
INNER JOIN tran_dau td ON kq.id_tran_dau = td.id
INNER JOIN vong_dau vd ON td.id_vong_dau = vd.id
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
INNER JOIN doi_bong db1 ON kq.id_doi = db1.id
INNER JOIN doi_bong db2 ON kq.id_doi_thu = db2.id
LEFT JOIN thong_ke_tran_dau tk ON kq.id = tk.id_ket_qua
WHERE td.ngay_gio IS NOT NULL
ORDER BY td.ngay_gio DESC
LIMIT 10;

-- View: Thống kê đội bóng
CREATE OR REPLACE VIEW v_thong_ke_doi_bong AS
SELECT 
    db.id AS 'ID Đội',
    db.ten_doi AS 'Tên đội',
    COUNT(DISTINCT kq.id_tran_dau) AS 'Số trận đã chơi',
    SUM(CASE WHEN kq.ket_qua = 'W' THEN 1 ELSE 0 END) AS 'Số trận thắng',
    SUM(CASE WHEN kq.ket_qua = 'L' THEN 1 ELSE 0 END) AS 'Số trận thua',
    SUM(CASE WHEN kq.ket_qua = 'D' THEN 1 ELSE 0 END) AS 'Số trận hòa',
    SUM(kq.ban_thang_ghi_duoc) AS 'Tổng bàn thắng',
    SUM(kq.ban_thua) AS 'Tổng bàn thua',
    AVG(tk.ty_le_so_huu_banh) AS 'Tỷ lệ kiểm soát bóng trung bình (%)',
    AVG(tk.xg) AS 'xG trung bình'
FROM doi_bong db
LEFT JOIN ket_qua_tran_dau kq ON db.id = kq.id_doi
LEFT JOIN thong_ke_tran_dau tk ON kq.id = tk.id_ket_qua
GROUP BY db.id, db.ten_doi
ORDER BY SUM(CASE WHEN kq.ket_qua = 'W' THEN 1 ELSE 0 END) DESC;

-- View: Các đội bóng trong mùa giải
CREATE OR REPLACE VIEW v_mua_giai_doi_bong AS
SELECT 
    mg.id AS 'ID Mùa giải',
    mg.ten_mua_giai AS 'Mùa giải',
    db.id AS 'ID Đội',
    db.ten_doi AS 'Tên đội',
    mgdb.vi_tri_bang_xep_hang AS 'Vị trí',
    mgdb.so_tran_thang AS 'Thắng',
    mgdb.so_tran_hoa AS 'Hòa',
    mgdb.so_tran_thua AS 'Thua',
    mgdb.tong_ban_thang AS 'Bàn thắng',
    mgdb.tong_ban_thua AS 'Bàn thua',
    mgdb.hieu_so AS 'Hiệu số',
    mgdb.tong_diem AS 'Điểm'
FROM mua_giai_doi_bong mgdb
INNER JOIN mua_giai mg ON mgdb.id_mua_giai = mg.id
INNER JOIN doi_bong db ON mgdb.id_doi_bong = db.id
ORDER BY mg.ten_mua_giai DESC, mgdb.tong_diem DESC, mgdb.hieu_so DESC;

-- View: Các vòng đấu trong mùa giải
CREATE OR REPLACE VIEW v_vong_dau_mua_giai AS
SELECT 
    vd.id AS 'ID Vòng đấu',
    mg.ten_mua_giai AS 'Mùa giải',
    vd.so_vong_dau AS 'Số vòng đấu',
    vd.so_tran_dau AS 'Số trận đấu',
    COUNT(td.id) AS 'Số trận đã diễn ra'
FROM vong_dau vd
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
LEFT JOIN tran_dau td ON vd.id = td.id_vong_dau
GROUP BY vd.id, mg.ten_mua_giai, vd.so_vong_dau, vd.so_tran_dau
ORDER BY mg.ten_mua_giai DESC, vd.so_vong_dau ASC;

-- View: Bàn thắng trong các trận đấu
CREATE OR REPLACE VIEW v_ban_thang_tran_dau AS
SELECT 
    sk.id AS 'ID Sự kiện',
    td.id_tran_dau AS 'ID Trận đấu',
    mg.ten_mua_giai AS 'Mùa giải',
    vd.so_vong_dau AS 'Vòng đấu',
    td.ngay_gio AS 'Ngày giờ',
    db_doi.ten_doi AS 'Đội ghi bàn',
    db_doi_thu.ten_doi AS 'Đội đối thủ',
    sk.ten_cau_thu AS 'Cầu thủ ghi bàn',
    sk.phut AS 'Phút',
    sk.chi_tiet AS 'Chi tiết',
    CASE 
        WHEN td.id_doi_nha = sk.id_doi_bong THEN 'Nhà'
        ELSE 'Khách'
    END AS 'Sân nhà/Khách'
FROM su_kien_tran_dau sk
INNER JOIN tran_dau td ON sk.id_tran_dau = td.id
INNER JOIN vong_dau vd ON td.id_vong_dau = vd.id
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
INNER JOIN doi_bong db_doi ON sk.id_doi_bong = db_doi.id
LEFT JOIN ket_qua_tran_dau kq ON td.id = kq.id_tran_dau AND sk.id_doi_bong = kq.id_doi
LEFT JOIN doi_bong db_doi_thu ON kq.id_doi_thu = db_doi_thu.id
WHERE sk.loai_su_kien = 'Goal'
ORDER BY td.ngay_gio DESC, sk.phut ASC;

-- View: Top cầu thủ ghi bàn
CREATE OR REPLACE VIEW v_top_cau_thu_ghi_ban AS
SELECT 
    sk.ten_cau_thu AS 'Cầu thủ',
    COUNT(*) AS 'Số bàn thắng',
    COUNT(DISTINCT sk.id_tran_dau) AS 'Số trận có bàn thắng',
    GROUP_CONCAT(DISTINCT mg.ten_mua_giai ORDER BY mg.ten_mua_giai DESC SEPARATOR ', ') AS 'Các mùa giải',
    MIN(sk.phut) AS 'Phút sớm nhất',
    MAX(sk.phut) AS 'Phút muộn nhất',
    AVG(sk.phut) AS 'Phút trung bình'
FROM su_kien_tran_dau sk
INNER JOIN tran_dau td ON sk.id_tran_dau = td.id
INNER JOIN vong_dau vd ON td.id_vong_dau = vd.id
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
WHERE sk.loai_su_kien = 'Goal' AND sk.ten_cau_thu IS NOT NULL
GROUP BY sk.ten_cau_thu
ORDER BY COUNT(*) DESC;

-- View: Điểm của mỗi đội bóng qua từng vòng
CREATE OR REPLACE VIEW v_diem_doi_bong_theo_vong AS
SELECT 
    mg.ten_mua_giai AS 'Mùa giải',
    vd.so_vong_dau AS 'Vòng đấu',
    db.id AS 'ID Đội',
    db.ten_doi AS 'Tên đội',
    kq.id_tran_dau AS 'ID Trận đấu',
    td.ngay_gio AS 'Ngày giờ',
    CASE 
        WHEN kq.la_doi_nha = 1 THEN db_home.ten_doi
        ELSE db_away.ten_doi
    END AS 'Đối thủ',
    kq.ket_qua AS 'Kết quả',
    kq.ban_thang_ghi_duoc AS 'Bàn thắng',
    kq.ban_thua AS 'Bàn thua',
    kq.tong_diem AS 'Điểm',
    -- Tính tổng điểm tích lũy
    SUM(kq.tong_diem) OVER (
        PARTITION BY mg.id, db.id 
        ORDER BY vd.so_vong_dau, td.ngay_gio 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS 'Tổng điểm tích lũy',
    -- Tính tổng bàn thắng tích lũy
    SUM(kq.ban_thang_ghi_duoc) OVER (
        PARTITION BY mg.id, db.id 
        ORDER BY vd.so_vong_dau, td.ngay_gio 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS 'Tổng bàn thắng tích lũy',
    -- Tính tổng bàn thua tích lũy
    SUM(kq.ban_thua) OVER (
        PARTITION BY mg.id, db.id 
        ORDER BY vd.so_vong_dau, td.ngay_gio 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS 'Tổng bàn thua tích lũy'
FROM ket_qua_tran_dau kq
INNER JOIN tran_dau td ON kq.id_tran_dau = td.id
INNER JOIN vong_dau vd ON td.id_vong_dau = vd.id
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
INNER JOIN doi_bong db ON kq.id_doi = db.id
LEFT JOIN doi_bong db_home ON td.id_doi_nha = db_home.id
LEFT JOIN doi_bong db_away ON td.id_doi_khach = db_away.id
ORDER BY mg.ten_mua_giai DESC, vd.so_vong_dau ASC, db.ten_doi ASC, td.ngay_gio ASC;

-- View: Bảng xếp hạng theo mùa giải và vòng đấu
CREATE OR REPLACE VIEW v_bang_xep_hang_theo_vong AS
SELECT 
    mg.ten_mua_giai AS 'Mùa giải',
    vd.so_vong_dau AS 'Vòng đấu',
    db.id AS 'ID Đội',
    db.ten_doi AS 'Tên đội',
    COUNT(DISTINCT kq.id_tran_dau) AS 'Số trận đã chơi',
    SUM(CASE WHEN kq.ket_qua = 'W' THEN 1 ELSE 0 END) AS 'Thắng',
    SUM(CASE WHEN kq.ket_qua = 'D' THEN 1 ELSE 0 END) AS 'Hòa',
    SUM(CASE WHEN kq.ket_qua = 'L' THEN 1 ELSE 0 END) AS 'Thua',
    SUM(kq.ban_thang_ghi_duoc) AS 'Bàn thắng',
    SUM(kq.ban_thua) AS 'Bàn thua',
    SUM(kq.ban_thang_ghi_duoc) - SUM(kq.ban_thua) AS 'Hiệu số',
    SUM(kq.tong_diem) AS 'Điểm'
FROM ket_qua_tran_dau kq
INNER JOIN tran_dau td ON kq.id_tran_dau = td.id
INNER JOIN vong_dau vd ON td.id_vong_dau = vd.id
INNER JOIN mua_giai mg ON vd.id_mua_giai = mg.id
INNER JOIN doi_bong db ON kq.id_doi = db.id
GROUP BY mg.id, mg.ten_mua_giai, vd.so_vong_dau, db.id, db.ten_doi
ORDER BY mg.ten_mua_giai DESC, vd.so_vong_dau ASC, SUM(kq.tong_diem) DESC, 
         (SUM(kq.ban_thang_ghi_duoc) - SUM(kq.ban_thua)) DESC;

-- ============================================
-- Kết thúc Schema
-- ============================================
