import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from PIL import Image

# --- Parameters ---
BG_IMAGE_PATH = "splash-bg.png"
TEXT_IMAGE_PATH = "splash-text-shading.png"
ALIVE_OUTER = np.array([251, 146, 128])  # #FB9280
DEAD_OUTER = np.array([247, 106, 106])   # #F76A6A
ALIVE_INNER = DEAD_OUTER
DEAD_INNER = ALIVE_OUTER
FADE_IN_START = 100
FADE_IN_END = 150
GRID_SHAPE = (128, 384)
SAVE_GIF = False

# --- Grid Init with Zonal Logic ---

def zoned_binary_grid(img_path):
    img = Image.open(img_path).convert("RGB").resize(GRID_SHAPE[::-1])
    arr = np.array(img)
    height, width, _ = arr.shape
    grid = np.zeros((height, width), dtype=int)

    top, bottom = 18, height - 17
    left, right = 12, width - 12

    for i in range(height):
        for j in range(width):
            pixel = tuple(arr[i, j])
            in_outer = (i < top or i >= bottom or j < left or j >= right)
            if in_outer and pixel == tuple(ALIVE_OUTER):
                grid[i, j] = 1
            elif not in_outer and pixel == tuple(ALIVE_INNER):
                grid[i, j] = 1
    return grid, arr

# --- Text Overlay ---

def load_text_overlay(img_path):
    img = Image.open(img_path).convert("RGBA").resize(GRID_SHAPE[::-1])
    return np.array(img)

def blend_text(frame, base_rgb, overlay_rgba):
    if frame < FADE_IN_START:
        return base_rgb
    alpha = min(1.0, (frame - FADE_IN_START) / (FADE_IN_END - FADE_IN_START))
    mask = overlay_rgba[..., 3] > 0
    blended = base_rgb.copy()
    blended[mask] = (
        (1 - alpha) * blended[mask] + alpha * overlay_rgba[mask, :3]
    ).astype(np.uint8)
    return blended

# --- Colorize Alive/Dead by Zone ---

def colorize(grid):
    rgb = np.zeros((grid.shape[0], grid.shape[1], 3), dtype=np.uint8)
    height, width = grid.shape
    top, bottom = 18, height - 17
    left, right = 12, width - 12
    for i in range(height):
        for j in range(width):
            in_outer = (i < top or i >= bottom or j < left or j >= right)
            if in_outer:
                rgb[i, j] = ALIVE_OUTER if grid[i, j] else DEAD_OUTER
            else:
                rgb[i, j] = ALIVE_INNER if grid[i, j] else DEAD_INNER
    return rgb

# --- Game of Life Step ---

def step(grid):
    new_grid = grid.copy()
    for i in range(grid.shape[0]):
        for j in range(grid.shape[1]):
            total = sum([
                grid[(i + di) % grid.shape[0], (j + dj) % grid.shape[1]]
                for di in [-1, 0, 1] for dj in [-1, 0, 1] if not (di == 0 and dj == 0)
            ])
            if grid[i, j] == 1:
                if total < 2 or total > 3:
                    new_grid[i, j] = 0
            else:
                if total == 3:
                    new_grid[i, j] = 1
    return new_grid

# --- Animation ---

def update(frame, img_disp, grid, text_overlay):
    grid[:] = step(grid)
    rgb = colorize(grid)
    blended = blend_text(frame, rgb, text_overlay)
    img_disp.set_data(blended)
    return img_disp,

# --- Run and Save ---

def run():
    grid, _ = zoned_binary_grid(BG_IMAGE_PATH)
    text_overlay = load_text_overlay(TEXT_IMAGE_PATH)
    fig, ax = plt.subplots()
    ax.axis('off')
    img_disp = ax.imshow(colorize(grid), interpolation='nearest')

    ani = animation.FuncAnimation(
        fig, update, fargs=(img_disp, grid, text_overlay),
        frames=200, interval=100, blit=True, repeat=False
    )

    if SAVE_GIF:
        ani.save("zoned_coral_life.gif", writer='pillow', fps=10)
    else:
        plt.show()

# --- Run it ---
if __name__ == "__main__":
    run()